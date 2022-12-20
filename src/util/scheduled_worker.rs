use std::error::Error;
use std::sync::{Arc, Mutex};
use log::{debug, info, warn};
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinHandle;
use tokio::time;

pub trait Worker {
    fn work(&mut self) -> Result<(), Box<dyn Error>>;
}

pub type MutexWorker = Arc<Mutex<dyn Worker + Send>>;

// Must be async as required by tokio::select!
async fn repeat(worker: &MutexWorker, period: Duration, mut rx: Receiver<()>) {
    let mut interval = time::interval(period);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let mut worker = worker.lock().unwrap();
                if let Err(e) = worker.work() {
                    warn!("Worker failed: {:?}, leave scheduler", e);
                    break;
                }
            },
            _ = rx.recv() => {
                debug!("Termination signal received, leave scheduler");
                break;
            }
        }
    }
}

pub fn spawn_scheduler(worker: &MutexWorker, rx: Receiver<()>, period: Duration) -> JoinHandle<()> {
    info!("Spawn scheduler");
    let worker = worker.clone();
    tokio::spawn(async move {
        repeat(&worker, period, rx).await;
    })
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::broadcast;
    use tokio::time::sleep;
    use crate::util::scheduled_worker::{MutexWorker, spawn_scheduler, Worker};

    struct TestWorker {
        counter: usize
    }

    impl TestWorker {
        fn new() -> Self {
            Self { counter: 0 }
        }
    }

    impl Worker for TestWorker {
        fn work(&mut self) -> Result<(), Box<dyn Error>> {
            self.counter += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_scheduler() {
        let worker= Arc::new(Mutex::new(TestWorker::new()));
        let cloned : MutexWorker = worker.clone();
        let (tx, rx) = broadcast::channel(1);
        let handle = spawn_scheduler(&cloned, rx, Duration::from_millis(1));
        sleep(Duration::from_millis(10)).await;
        assert!(tx.send(()).is_ok()); // Terminate scheduler
        assert!(handle.await.is_ok());
        let worker = worker.lock().unwrap();
        assert!(worker.counter > 0);
    }
}