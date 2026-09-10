use std::{
    sync::mpsc::{self, RecvTimeoutError, Sender},
    thread::{self, JoinHandle},
    time::Duration,
};

use super::{ArtifactStore, TimeoutSnapshot};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WatchdogOutcome {
    Cancelled,
    TimedOut { artifact: std::path::PathBuf },
}

pub struct Watchdog {
    cancel: Option<Sender<()>>,
    worker: Option<JoinHandle<Result<WatchdogOutcome, String>>>,
}

impl Watchdog {
    pub fn start(
        timeout: Duration,
        store: ArtifactStore,
        snapshot: impl FnOnce() -> TimeoutSnapshot + Send + 'static,
    ) -> Result<Self, String> {
        if timeout.is_zero() {
            return Err("watchdog timeout must be greater than zero".to_string());
        }
        let (cancel, receiver) = mpsc::channel();
        let worker = thread::Builder::new()
            .name("prerelease-watchdog".to_string())
            .spawn(move || match receiver.recv_timeout(timeout) {
                Ok(()) | Err(RecvTimeoutError::Disconnected) => Ok(WatchdogOutcome::Cancelled),
                Err(RecvTimeoutError::Timeout) => {
                    let artifact = store.write_timeout(&snapshot())?;
                    Ok(WatchdogOutcome::TimedOut { artifact })
                }
            })
            .map_err(|error| error.to_string())?;
        Ok(Self {
            cancel: Some(cancel),
            worker: Some(worker),
        })
    }

    pub fn cancel(mut self) -> Result<WatchdogOutcome, String> {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
        self.join()
    }

    pub fn wait(mut self) -> Result<WatchdogOutcome, String> {
        let cancel = self.cancel.take();
        let result = self.join();
        drop(cancel);
        result
    }

    fn join(&mut self) -> Result<WatchdogOutcome, String> {
        self.worker
            .take()
            .ok_or_else(|| "watchdog worker was already joined".to_string())?
            .join()
            .map_err(|_| "watchdog worker panicked".to_string())?
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
