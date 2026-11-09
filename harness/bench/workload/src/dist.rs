/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use rand::{RngExt, rngs::SmallRng};
use rand_distr::{Distribution, Zipf};
use serde::Deserialize;
use std::fmt;

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(untagged)]
pub enum DistSpec {
    Fixed(u64),
    Shaped(Shape),
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(tag = "dist", rename_all = "kebab-case")]
pub enum Shape {
    Uniform { min: u64, max: u64 },
    LogUniform { min: u64, max: u64 },
    Exp { mean: u64 },
}

#[derive(Clone, Copy, Debug)]
pub enum Dist {
    Fixed(u64),
    Uniform { min: u64, max: u64 },
    LogUniform { ln_min: f64, ln_max: f64 },
    Exp { mean: f64 },
}

#[derive(Debug)]
pub struct DistError(&'static str);

impl fmt::Display for DistError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl TryFrom<DistSpec> for Dist {
    type Error = DistError;

    fn try_from(spec: DistSpec) -> Result<Self, Self::Error> {
        match spec {
            DistSpec::Fixed(value) => Ok(Dist::Fixed(value)),
            DistSpec::Shaped(Shape::Uniform { min, max }) if min <= max => {
                Ok(Dist::Uniform { min, max })
            }
            DistSpec::Shaped(Shape::LogUniform { min, max }) if 0 < min && min <= max => {
                Ok(Dist::LogUniform {
                    ln_min: (min as f64).ln(),
                    ln_max: (max as f64).ln(),
                })
            }
            DistSpec::Shaped(Shape::Exp { mean }) => Ok(Dist::Exp { mean: mean as f64 }),
            DistSpec::Shaped(Shape::Uniform { .. }) => Err(DistError("uniform needs min <= max")),
            DistSpec::Shaped(Shape::LogUniform { .. }) => {
                Err(DistError("log-uniform needs 0 < min <= max"))
            }
        }
    }
}

impl Dist {
    pub fn sample(&self, rng: &mut SmallRng) -> u64 {
        match *self {
            Dist::Fixed(value) => value,
            Dist::Uniform { min, max } => rng.random_range(min..=max),
            Dist::LogUniform { ln_min, ln_max } => {
                rng.random_range(ln_min..=ln_max).exp().round() as u64
            }
            Dist::Exp { mean } => exponential(rng, mean).round() as u64,
        }
    }
}

#[inline]
pub fn exponential(rng: &mut SmallRng, mean: f64) -> f64 {
    -mean * (1.0 - rng.random::<f64>()).ln()
}

#[derive(Clone, Copy, Debug)]
pub enum Popularity {
    Uniform,
    Zipf { zipf: Zipf<f64>, exponent: f64 },
}

impl Popularity {
    pub fn parse(spec: &str, population: u64) -> Result<Popularity, DistError> {
        let (name, argument) = spec.split_once(':').unwrap_or((spec, ""));
        hashify::fnc_map!(name.as_bytes(),
            "uniform" => {
                return Ok(Popularity::Uniform);
            },
            "zipf" => {
                let exponent = argument
                    .parse::<f64>()
                    .map_err(|_| DistError("zipf exponent is not a number"))?;
                return Zipf::new(population as f64, exponent)
                    .map(|zipf| Popularity::Zipf { zipf, exponent })
                    .map_err(|_| DistError("invalid zipf parameters"));
            },
            _ => {}
        );
        Err(DistError("popularity is uniform or zipf:<exponent>"))
    }

    #[inline]
    pub fn sample(&self, rng: &mut SmallRng, population: u64) -> u64 {
        match self {
            Popularity::Uniform => rng.random_range(0..population),
            Popularity::Zipf { zipf, .. } => (zipf.sample(rng) as u64).saturating_sub(1),
        }
    }
}
