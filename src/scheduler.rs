use std::time::Duration;
use std::thread;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::VecDeque;
use std::cmp;

use futures_cpupool::{Builder, CpuPool};
use futures::{Async, Future, BoxFuture};

use error::*;
use utils::format_error_chain;

// use scheduler::futures::Future;
// use scheduler::futures_cpupool::{CpuPool, CpuFuture};

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

// fn run_task<T: ScheduledTask>(task: &T, pool: &CpuPool) -> CpuFuture<(), error::Error> {
//     let ret_value = pool.spawn_fn(|| {
//         let result = futures::done(task.run());
//         result
//     });
//
//     ret_value
// }

fn scheduler_clock_loop<I, T>(period: Duration, tasks: Arc<RwLock<Vec<Arc<(I, T)>>>>,
                              cpu_pool: CpuPool, should_stop: Arc<AtomicBool>)
    where I: Eq + Send + Sync + 'static,
          T: ScheduledTask {
    let mut index = 0;
    let mut futures: VecDeque<BoxFuture<(), Error>> = VecDeque::new();
    thread::sleep(Duration::from_millis(100));  // Wait for task enqueuing
    while !should_stop.load(Ordering::Relaxed) {
        /// Removes completed futures from the deque
        loop {
            match futures.front_mut() {
                Some(f) => {
                    match f.poll() {
                        Ok(Async::NotReady) => { trace!("Future not ready"); break; },
                        Ok(Async::Ready(_)) => trace!("Future completed correctly"),
                        Err(e) => format_error_chain(e),
                    };
                },
                None => break,
            };
            futures.pop_front();
        }
        let n_tasks = {
            let tasks = tasks.read().unwrap();
            let task_clone = tasks[index].clone();
            let f = cpu_pool.spawn_fn(move || {
                task_clone.1.run()
            });
            futures.push_back(f.boxed());
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
