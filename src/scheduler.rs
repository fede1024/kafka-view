use std::time::Duration;
use std::thread;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::VecDeque;
use std::cmp;

use futures_cpupool::{Builder, CpuPool};
use futures::{Future, BoxFuture};

use error::*;


pub trait ScheduledTask: Send + Sync + 'static {
    fn run(&self) -> Result<()>;
}

pub struct Scheduler<I: Eq + Send + Sync + 'static, T: ScheduledTask> {
    tasks: Arc<RwLock<Vec<Arc<(I, T)>>>>,
    period: Duration,
    thread: Option<thread::JoinHandle<()>>,
    should_stop: Arc<AtomicBool>,
    cpu_pool: CpuPool,
}

impl<I: Eq + Send + Sync + 'static, T: ScheduledTask> Scheduler<I, T> {
    pub fn new(period: Duration, pool_size: usize) -> Scheduler<I, T> {
        Scheduler {
            tasks: Arc::new(RwLock::new(Vec::new())),
            period: period,
            thread: None,
            should_stop: Arc::new(AtomicBool::new(false)),
            cpu_pool: Builder::new().pool_size(pool_size).create(),
        }
    }

    pub fn add_task(&mut self, id: I, task: T) {
        if self.thread.is_none() {
            let tasks_clone = self.tasks.clone();
            let period_clone = self.period.clone();
            let should_stop_clone = self.should_stop.clone();
            let cpu_pool_clone = self.cpu_pool.clone();
            let builder = thread::Builder::new().name("Scheduler".into());
            let thread = builder.spawn(move || scheduler_clock_loop(period_clone, tasks_clone,
                                                                    cpu_pool_clone, should_stop_clone))
                .unwrap();
            self.thread = Some(thread);
        }
        let mut tasks = self.tasks.write().unwrap();
        for i in 0..tasks.len() {
            if tasks[i].0 == id {
                tasks[i] = Arc::new((id, task));
                return;
            }
        }
        tasks.push(Arc::new((id, task)));
    }

    pub fn stop(&self) {
        self.should_stop.store(true, Ordering::Relaxed);
    }
}

fn scheduler_clock_loop<I, T>(period: Duration, tasks: Arc<RwLock<Vec<Arc<(I, T)>>>>,
                              cpu_pool: CpuPool, should_stop: Arc<AtomicBool>)
    where I: Eq + Send + Sync + 'static,
          T: ScheduledTask {
    let mut index = 0;
    let mut futures: VecDeque<(Arc<AtomicBool>, BoxFuture<(), Error>)> = VecDeque::new();
    thread::sleep(Duration::from_millis(100));  // Wait for task enqueuing
    while !should_stop.load(Ordering::Relaxed) {
        // Removes completed futures from the deque
        loop {
            match futures.pop_front() {
                Some(f) => {
                    if f.0.load(Ordering::Relaxed) {
                        match f.1.wait() {
                            Ok(_) => trace!("Future completed correctly, {} pending", futures.len()),
                            Err(e) => format_error_chain!(e),
                        };
                    } else {
                        trace!("Future not ready");
                        futures.push_back(f);
                        break;
                    }
                },
                None => break,
            };
        }
        let n_tasks = {
            let tasks = tasks.read().unwrap();
            let complete = Arc::new(AtomicBool::new(false));
            let task_clone = tasks[index].clone();
            let complete_clone = complete.clone();
            let f = cpu_pool.spawn_fn(move || {
                let res = task_clone.1.run();
                complete_clone.store(true, Ordering::Relaxed);
                res
            });
            futures.push_back((complete, f.boxed()));
            index = (index + 1) % tasks.len();
            tasks.len()
        };
        let interval = cmp::max(period / (n_tasks as u32), Duration::from_millis(100));
        thread::sleep(interval);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}
