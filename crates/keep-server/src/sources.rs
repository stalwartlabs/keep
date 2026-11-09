/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::any::TypeId;
use std::error::Error as _;
use std::ffi::{OsStr, OsString};
use std::fmt::{self, Display, Write};
use std::fs;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use clap::error::{ContextKind, Error, ErrorKind};
use clap::parser::ValueSource;
use clap::{Arg, ArgAction, ArgMatches, Command, value_parser};
use toml::{Table, Value};

pub const CONFIG: &str = "config";

pub const ENV_PREFIX: &str = "KEEP_";

pub struct Alias {
    pub arg: &'static str,
    pub var: &'static str,
}

enum Origin<'a> {
    Env(&'a str),
    File { path: &'a Path, key: &'a str },
}

impl Display for Origin<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Origin::Env(var) => f.write_str(var),
            Origin::File { path, key } => {
                write!(f, "config file {}, key `{key}`", path.display())
            }
        }
    }
}

enum Expected {
    Flag,
    Integer,
    Text,
    List,
}

impl Expected {
    fn of(arg: &Arg) -> Option<Expected> {
        match arg.get_action() {
            ArgAction::SetTrue => Some(Expected::Flag),
            ArgAction::Append => Some(Expected::List),
            ArgAction::Set if integer(arg) => Some(Expected::Integer),
            ArgAction::Set => Some(Expected::Text),
            _ => None,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Expected::Flag => "a boolean",
            Expected::Integer => "an integer or a string of digits",
            Expected::Text => "a string",
            Expected::List => "an array of strings",
        }
    }

    fn env_tokens(
        &self,
        tokens: &mut Vec<OsString>,
        long: &str,
        value: &OsStr,
        origin: &Origin,
    ) -> Result<(), Error> {
        match self {
            Expected::Flag => match value.to_str().and_then(|text| text.parse().ok()) {
                Some(true) => tokens.push(flag(long)),
                Some(false) => {}
                None => {
                    return Err(Error::raw(
                        ErrorKind::InvalidValue,
                        format!(
                            "{origin}: invalid value '{}': expected true or false",
                            value.display()
                        ),
                    ));
                }
            },
            Expected::Integer | Expected::Text | Expected::List => {
                tokens.push(option(long, value));
            }
        }
        Ok(())
    }

    fn file_tokens(
        &self,
        tokens: &mut Vec<OsString>,
        long: &str,
        value: &Value,
        origin: &Origin,
    ) -> Result<(), Error> {
        match (self, value) {
            (Expected::Flag, Value::Boolean(true)) => tokens.push(flag(long)),
            (Expected::Flag, Value::Boolean(false)) => {}
            (Expected::Integer, Value::Integer(number)) => {
                tokens.push(option(long, number.to_string()));
            }
            (Expected::Integer, Value::String(text)) => tokens.push(option(long, text)),
            (Expected::Text, Value::String(text)) => tokens.push(option(long, text)),
            (Expected::List, Value::Array(items)) => {
                for item in items {
                    let Value::String(text) = item else {
                        return Err(self.mistyped(item, origin));
                    };
                    tokens.push(option(long, text));
                }
            }
            _ => return Err(self.mistyped(value, origin)),
        }
        Ok(())
    }

    #[cold]
    fn mistyped(&self, value: &Value, origin: &Origin) -> Error {
        Error::raw(
            ErrorKind::InvalidValue,
            format!(
                "{origin}: expected {}, found {}",
                self.name(),
                value.type_str()
            ),
        )
    }
}

fn integer(arg: &Arg) -> bool {
    let parsed = arg.get_value_parser().type_id();
    [
        TypeId::of::<u16>(),
        TypeId::of::<u32>(),
        TypeId::of::<u64>(),
        TypeId::of::<usize>(),
        TypeId::of::<NonZeroU64>(),
    ]
    .into_iter()
    .any(|integer| parsed == integer)
}

fn flag(long: &str) -> OsString {
    let mut token = OsString::with_capacity(long.len() + 2);
    token.push("--");
    token.push(long);
    token
}

fn option(long: &str, value: impl AsRef<OsStr>) -> OsString {
    let value = value.as_ref();
    let mut token = OsString::with_capacity(long.len() + value.len() + 3);
    token.push("--");
    token.push(long);
    token.push("=");
    token.push(value);
    token
}

fn env_name<'a>(name: &'a mut String, long: &str) -> &'a str {
    name.clear();
    name.push_str(ENV_PREFIX);
    name.extend(long.chars().map(|char| match char {
        '-' => '_',
        char => char.to_ascii_uppercase(),
    }));
    name
}

fn check(arg: &Arg, tokens: &[OsString], origin: &Origin) -> Result<(), Error> {
    Command::new("keep")
        .no_binary_name(true)
        .arg(arg.clone())
        .try_get_matches_from(tokens)
        .map(drop)
        .map_err(|err| {
            let mut message = origin.to_string();
            if let Some(value) = err.get(ContextKind::InvalidValue) {
                let _ = write!(message, ": invalid value '{value}'");
            }
            let _ = match (err.source(), err.get(ContextKind::ValidValue)) {
                (Some(source), _) => write!(message, ": {source}"),
                (None, Some(valid)) => write!(message, ": possible values {valid}"),
                (None, None) => write!(message, ": {}", err.kind()),
            };
            Error::raw(err.kind(), message)
        })
}

fn load(path: &Path) -> Result<Table, Error> {
    let text = fs::read_to_string(path).map_err(|err| {
        Error::raw(
            ErrorKind::Io,
            format!("config file {}: {err}", path.display()),
        )
    })?;
    text.parse().map_err(|err: toml::de::Error| {
        let mut message = format!("config file {}", path.display());
        if let Some(before) = err.span().and_then(|span| text.get(..span.start)) {
            let line = before.bytes().filter(|&byte| byte == b'\n').count() + 1;
            let column = before
                .rsplit_once('\n')
                .map_or(before, |(_, last)| last)
                .chars()
                .count()
                + 1;
            let _ = write!(message, ", line {line}, column {column}");
        }
        let _ = write!(message, ": {}", err.message());
        Error::raw(ErrorKind::InvalidValue, message)
    })
}

pub fn matches(
    command: Command,
    argv: Vec<OsString>,
    aliases: &[Alias],
    env: &dyn Fn(&str) -> Option<OsString>,
) -> Result<ArgMatches, Error> {
    let mut command = command.arg(
        Arg::new(CONFIG)
            .long(CONFIG)
            .value_name("PATH")
            .value_parser(value_parser!(PathBuf))
            .help("TOML file of settings keyed by flag name, as in max-lock-ttl = 60; every setting is also read from KEEP_<FLAG>, as in KEEP_MAX_LOCK_TTL; flags override the variables, which override the file"),
    );
    let given = command.try_get_matches_from_mut(&argv)?;
    let layered =
        layer(&command, &given, argv, aliases, env).map_err(|err| err.format(&mut command))?;
    command.try_get_matches_from(layered)
}

fn layer(
    command: &Command,
    given: &ArgMatches,
    argv: Vec<OsString>,
    aliases: &[Alias],
    env: &dyn Fn(&str) -> Option<OsString>,
) -> Result<Vec<OsString>, Error> {
    let mut name = String::new();
    let path = match given.get_one::<PathBuf>(CONFIG) {
        Some(path) => Some(path.clone()),
        None => env(env_name(&mut name, CONFIG)).map(PathBuf::from),
    };
    let mut file = match path.as_deref() {
        Some(path) => Some((path, load(path)?)),
        None => None,
    };
    if let Some((path, table)) = &file
        && table.contains_key(CONFIG)
    {
        return Err(Error::raw(
            ErrorKind::UnknownArgument,
            format!(
                "config file {}: `{CONFIG}` cannot be set in a config file",
                path.display()
            ),
        ));
    }
    let mut argv = argv.into_iter();
    let mut layered: Vec<OsString> = argv.next().into_iter().collect();
    let mut tokens = Vec::new();
    for arg in command.get_arguments() {
        let id = arg.get_id().as_str();
        let (Some(long), Some(expected)) = (arg.get_long(), Expected::of(arg)) else {
            continue;
        };
        if id == CONFIG {
            continue;
        }
        let var = env_name(&mut name, long);
        let alias = aliases
            .iter()
            .find(|alias| alias.arg == id && env(alias.var).is_some());
        let mut taken = given.value_source(id) == Some(ValueSource::CommandLine);
        if let Some(value) = env(var) {
            if let Some(alias) = alias {
                return Err(Error::raw(
                    ErrorKind::ArgumentConflict,
                    format!("{var} and {} are both set", alias.var),
                ));
            }
            let origin = Origin::Env(var);
            tokens.clear();
            expected.env_tokens(&mut tokens, long, &value, &origin)?;
            check(arg, &tokens, &origin)?;
            if !taken {
                layered.append(&mut tokens);
                taken = true;
            }
        }
        taken |= alias.is_some();
        if let Some((path, table)) = &mut file
            && let Some(value) = table.remove(long)
        {
            let origin = Origin::File { path, key: long };
            tokens.clear();
            expected.file_tokens(&mut tokens, long, &value, &origin)?;
            check(arg, &tokens, &origin)?;
            if !taken {
                layered.append(&mut tokens);
            }
        }
    }
    if let Some((path, table)) = &file
        && let Some(key) = table.keys().next()
    {
        return Err(Error::raw(
            ErrorKind::UnknownArgument,
            format!("config file {}: unknown key `{key}`", path.display()),
        ));
    }
    layered.extend(argv);
    Ok(layered)
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-server/sources.rs"]
mod tests;
