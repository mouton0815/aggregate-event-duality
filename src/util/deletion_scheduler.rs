use std::error::Error;
use std::sync::{Arc, Mutex};
use log::{debug, info, warn};
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinHandle;
use tokio::time;

pub trait DeletionTask {
    fn delete(&mut self, created_before: Duration) -> Result<(), Box<dyn Error>>;
}

pub type MutexDeletionTask = Arc<Mutex<dyn DeletionTask + Send>>;

// Must be async as required by tokio::select!
async fn repeat(task: &MutexDeletionTask, period: Duration, mut rx: Receiver<()>) {
    let mut interval = time::interval(period);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let mut task = task.lock().unwrap();
                if let Err(e) = task.delete(period) {
                    warn!("Deletion task failed: {:?}, leave scheduler", e);
                    break;
                }
            },
            _ = rx.recv() => {
                debug!("Termination signal received, leave deletion scheduler");
                break;
            }
        }
    }
}

pub fn spawn_deletion_scheduler(task: &MutexDeletionTask, rx: Receiver<()>, period: Duration) -> JoinHandle<()> {
    info!("Spawn deletion scheduler");
    let task = task.clone();
    tokio::spawn(async move {
        repeat(&task, period, rx).await;
    })
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::broadcast;
    use tokio::time::sleep;
    use crate::util::deletion_scheduler::{MutexDeletionTask, spawn_deletion_scheduler, DeletionTask};

    struct TestTask {
        counter: u128
    }

    impl TestTask {
        fn new() -> Self {
            Self { counter: 0 }
        }
    }

    impl DeletionTask for TestTask {
        fn delete(&mut self, created_before: Duration) -> Result<(), Box<dyn Error>> {
            self.counter += created_before.as_millis();
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_scheduler() {
        let task = Arc::new(Mutex::new(TestTask::new()));
        let cloned : MutexDeletionTask = task.clone();
        let (tx, rx) = broadcast::channel(1);
        let handle = spawn_deletion_scheduler(&cloned, rx, Duration::from_millis(1));
        sleep(Duration::from_millis(10)).await;
        assert!(tx.send(()).is_ok()); // Terminate scheduler
        assert!(handle.await.is_ok());
        let task = task.lock().unwrap();
        assert!(task.counter > 0); // TestTask::delete() was called at least once
    }
}