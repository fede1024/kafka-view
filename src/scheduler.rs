// extern crate futures;
// extern crate futures_cpupool;

use std::time::{Duration, Instant};
use std::thread;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};

use error::*;
use utils::format_error_chain;

// use scheduler::futures::Future;
// use scheduler::futures_cpupool::{CpuPool, CpuFuture};

pub trait ScheduledTask: Send + Sync + 'static {
    fn run(&self) -> Result<()>;
}

pub struct Scheduler<I: Eq + Send + Sync + 'static, T: ScheduledTask> {
    tasks: Arc<RwLock<Vec<(I, T)>>>,
    period: Duration,
    thread: Option<thread::JoinHandle<()>>,
    should_stop: Arc<AtomicBool>,
}

impl<I: Eq + Send + Sync + 'static, T: ScheduledTask> Scheduler<I, T> {
    pub fn new(period: Duration) -> Scheduler<I, T> {
        Scheduler {
            tasks: Arc::new(RwLock::new(Vec::new())),
            period: period,
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

// fn run_task<T: ScheduledTask>(task: &T, pool: &CpuPool) -> CpuFuture<(), error::Error> {
//     let ret_value = pool.spawn_fn(|| {
//         let result = futures::done(task.run());
//         result
//     });
//
//     ret_value
// }

fn scheduler_clock_loop<I: Eq, T: ScheduledTask>(period: Duration, tasks: Arc<RwLock<Vec<(I, T)>>>, should_stop: Arc<AtomicBool>) {
    let mut index = 0;
    // let pool = CpuPool::new(4);
    thread::sleep(Duration::from_millis(100));  // Wait for task enqueuing
    while !should_stop.load(Ordering::Relaxed) {
        let start_time = Instant::now();
        let mut interval;
        {
            let tasks = tasks.read().unwrap();
            if index >= tasks.len() {
                index = 0;
            }
            // let boh = run_task(&tasks[index].1, &pool);
            // info!(">> {:?}", boh.wait());
            tasks[index].1.run().map_err(format_error_chain);
            let elapsed_time = Instant::now() - start_time;
            interval = if tasks.len() > 0 {
                period / (tasks.len() as u32)
            } else {
                Duration::from_secs(1)
            };
            if interval > elapsed_time {
                interval -= elapsed_time;
            } else {
                interval = Duration::from_secs(0);
            }
        }
        index += 1;
        thread::sleep(interval);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}
