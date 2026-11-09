/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::{
    clock::{Clock, NANOS_PER_MILLI, NANOS_PER_SEC, Window},
    kv::worker::KvContext,
    stream::StreamStats,
};
use std::{
    fmt::Write as _,
    io::{self, Write},
    sync::{
        Arc,
        atomic::Ordering,
        mpsc::{self, RecvTimeoutError},
    },
    thread,
    time::Duration,
};

pub struct Progress {
    stop: mpsc::Sender<()>,
    thread: thread::JoinHandle<()>,
}

pub struct Sources {
    pub node: u16,
    pub clock: Clock,
    pub window: Window,
    pub kv: Option<Arc<KvContext>>,
    pub stream: Option<Arc<StreamStats>>,
}

impl Progress {
    pub fn spawn<W: Write + Send + 'static>(
        interval: Duration,
        sources: Sources,
        out: W,
    ) -> io::Result<Self> {
        let (stop, stopped) = mpsc::channel();
        let thread = thread::Builder::new()
            .name(format!("ms-{}-progress", sources.node))
            .spawn(move || sources.print_until(interval, &stopped, out))?;
        Ok(Progress { stop, thread })
    }

    pub fn stop(self) -> thread::JoinHandle<()> {
        drop(self.stop);
        self.thread
    }
}

impl Sources {
    fn print_until<W: Write>(&self, interval: Duration, stopped: &mpsc::Receiver<()>, mut out: W) {
        let mut line = String::new();
        while let Err(RecvTimeoutError::Timeout) = stopped.recv_timeout(interval) {
            line.clear();
            self.write_line(self.clock.now(), &mut line);
            if out.write_all(line.as_bytes()).is_err() {
                return;
            }
        }
    }

    fn write_line(&self, now: u64, line: &mut String) {
        let phase = match (now < self.window.start, now < self.window.end) {
            (true, _) => "warmup",
            (false, true) => "measure",
            (false, false) => "drain",
        };
        let _ = write!(line, "node {} {phase}", self.node);
        if let Some(kv) = &self.kv {
            let errors = kv
                .stats
                .ops
                .iter()
                .map(|op| op.errors.load(Ordering::Relaxed))
                .sum::<u64>();
            let _ = write!(
                line,
                " kv completed {} errors {errors}",
                kv.stats.completed.load(Ordering::Relaxed)
            );
        }
        if let Some(stream) = &self.stream {
            let counters = &stream.counters;
            let _ = write!(
                line,
                " events published {} received {} gaps {} acks {}",
                counters.events_published.load(Ordering::Relaxed),
                counters.events_received.load(Ordering::Relaxed),
                counters.gaps.load(Ordering::Relaxed),
                counters.acks_received.load(Ordering::Relaxed),
            );
        }
        let _ = writeln!(
            line,
            " at {}.{:03}",
            now / NANOS_PER_SEC,
            now % NANOS_PER_SEC / NANOS_PER_MILLI
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn sources(stream: Option<Arc<StreamStats>>) -> Sources {
        let clock = Clock::start();
        Sources {
            node: 3,
            clock,
            window: Window::new(5 * NANOS_PER_SEC, Duration::from_secs(10)),
            kv: None,
            stream,
        }
    }

    // The line keeps the `node N PHASE` prefix and counter order that the progress parsers read and ends with the timestamp.
    #[test]
    fn line_keeps_parsed_prefix_and_ends_with_timestamp() {
        let progress = sources(Some(Arc::new(StreamStats::default())));
        let mut line = String::new();
        progress.write_line(7 * NANOS_PER_SEC + 42 * NANOS_PER_MILLI, &mut line);
        assert_eq!(
            line,
            "node 3 measure events published 0 received 0 gaps 0 acks 0 at 7.042\n"
        );
        line.clear();
        progress.write_line(NANOS_PER_SEC, &mut line);
        assert_eq!(
            line,
            "node 3 warmup events published 0 received 0 gaps 0 acks 0 at 1.000\n"
        );
        line.clear();
        progress.write_line(20 * NANOS_PER_SEC + 5, &mut line);
        assert_eq!(
            line,
            "node 3 drain events published 0 received 0 gaps 0 acks 0 at 20.000\n"
        );
    }

    struct BlockedWriter {
        entered: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
    }

    impl Write for BlockedWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let _ = self.entered.send(());
            let _ = self.release.recv();
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    // A progress write stuck in the kernel (a stalled stderr) must not hold up the tokio worker that runs the load.
    #[test]
    fn blocked_output_does_not_stall_a_single_worker_runtime() {
        let (entered_tx, entered) = mpsc::channel();
        let (release, release_rx) = mpsc::channel();
        let writer = BlockedWriter {
            entered: entered_tx,
            release: release_rx,
        };
        let progress = Progress::spawn(Duration::from_millis(1), sources(None), writer)
            .expect("progress thread");
        entered
            .recv_timeout(Duration::from_secs(5))
            .expect("first progress write");

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_time()
            .build()
            .expect("runtime");
        let started = Instant::now();
        runtime.block_on(async {
            let ticks = tokio::spawn(async {
                for _ in 0..20 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            });
            ticks.await.expect("ticks task");
        });
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "runtime stalled for {:?}",
            started.elapsed()
        );

        let handle = progress.stop();
        drop(release);
        handle.join().expect("progress thread exits");
    }
}
