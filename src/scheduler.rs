use std::time::Duration;
use std::thread;
use std::thread::{JoinHandle, Thread};
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};

use error::*;

pub trait ScheduledTask: Send + Sync + 'static {
    fn run(&self) -> Result<()>;
}

pub struct Scheduler<I: Eq + Send + Sync + 'static, T: ScheduledTask> {
    tasks: Arc<RwLock<Vec<(I, T)>>>,
    period: Duration,
    index: usize,
    thread: Option<JoinHandle<()>>,
    should_stop: Arc<AtomicBool>,
}

impl<I: Eq + Send + Sync + 'static, T: ScheduledTask> Scheduler<I, T> {
    pub fn new(period: Duration) -> Scheduler<I, T> {
        Scheduler {
            tasks: Arc::new(RwLock::new(Vec::new())),
            period: period,
            index: 0,
            thread: None,
            should_stop: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn add_task(&mut self, id: I, task: T) {
        if self.thread.is_none() {
            let tasks_clone = self.tasks.clone();
            let period_clone = self.period.clone();
            let should_stop_clone = self.should_stop.clone();
            let builder = thread::Builder::new().name("Scheduler".into());
            let thread = builder.spawn(move || scheduler_clock_loop(period_clone, tasks_clone, should_stop_clone)).unwrap();
            self.thread = Some(thread);
        }
        let mut tasks = self.tasks.write().unwrap();
        for i in 0..tasks.len() {
            if tasks[i].0 == id {
                tasks[i].1 = task;
                return;
            }
        }
        tasks.push((id, task));
    }

    pub fn stop(&self) {
        self.should_stop.store(true, Ordering::Relaxed);
    }
}

fn scheduler_clock_loop<I: Eq, T: ScheduledTask>(period: Duration, tasks: Arc<RwLock<Vec<(I, T)>>>, should_stop: Arc<AtomicBool>) {
    let mut index = 0;
    while !should_stop.load(Ordering::Relaxed) {
        let mut interval = Duration::from_secs(0);
        {
            let tasks = tasks.read().unwrap();
            if let Err(ref e) = tasks[index].1.run() {
                error!("error: {}", e);

                for e in e.iter().skip(1) {
                    error!("caused by: {}", e);
                }

                if let Some(backtrace) = e.backtrace() {
                    error!("backtrace: {:?}", backtrace);
                }
            }
            index += 1;
            if index >= tasks.len() {
                index = 0;
            }
            interval = period / (tasks.len() as u32);
        }
        thread::sleep(interval);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}
