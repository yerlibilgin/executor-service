use std::thread;
use std::sync::mpsc::{channel, sync_channel, Sender, SyncSender};
use crate::EventType::{*};
use std::sync::{Arc, Mutex, Condvar};


pub type Runnable = Box<dyn Send + Sync + FnOnce() -> ()>;

enum EventType {
  Execute(Runnable),
  ExecuteInner((Sender<EventType>, Runnable)),
  Quit,
}

pub struct ExecutorService {
  dispatcher: SyncSender<EventType>,
}

impl ExecutorService {
  pub fn submit(&mut self, fun: Runnable) {
    self.dispatcher.send(Execute(fun)).unwrap();
  }
}

impl Drop for ExecutorService {
  fn drop(&mut self) {
    self.dispatcher.send(EventType::Quit).unwrap();
  }
}


pub struct Executors;

impl Executors {
  pub fn new_fixed_thread_pool(thread_count: u32) -> ExecutorService {
    let mut guarded_count= thread_count;
    if guarded_count > 80 {
      guarded_count = 80;
    }

    let available = Arc::new(Mutex::new(vec![]));

    let (sender, receiver) = sync_channel::<EventType>(1);

    let pair = Arc::new(Condvar::new());

    for i in 0..guarded_count {
      let (s, r) = channel::<EventType>();

      if let Ok(mut lock) = available.lock() {
        lock.push(s);
      }

      let vec_clone = available.clone();
      let cv_clone = pair.clone();
      thread::Builder::new()
        .name(format!("Thread-{:}", i)).spawn(move || {
        loop {
          //println!("{:} Waiting for job", thread::current().name().unwrap());
          let fun = r.recv().unwrap();
          //println!("{:} Received func", thread::current().name().unwrap());
          match fun {
            ExecuteInner((sender, fun)) => {
              fun();
              //println!("{:} Send result", thread::current().name().unwrap());
              if let Ok(mut lock) = vec_clone.lock() {
                lock.push(sender);
                //println!("Notify waiters for finish");
                cv_clone.notify_all();
              }
            }
            Quit => {
              println!("{:} Received exit", thread::current().name().unwrap());
              break;
            }
            _ => {}
          }
        }
        //println!("{:} Loop done", thread::current().name().unwrap())
      }).unwrap();
    }

    thread::Builder::new()
      .name("Dispatcher".into())
      .spawn(move || {
        loop {
          match receiver.recv().unwrap() {
            Execute(func) => {
              if let Ok(lock) = available.lock() {
                if lock.is_empty() {
                  let _ = pair.wait(lock);
                }
              };

              if let Ok(mut lock) = available.lock() {
                //println!("Available: {:}", lock.len());
                let the_sender = lock.pop().unwrap();
                //println!("Available: {:}", lock.len());
                the_sender.send(ExecuteInner((the_sender.clone(), func))).unwrap();
              };
            }
            Quit => {
              //println!("Dispatcher received Quit");
              if let Ok(lock) = available.lock() {
                for x in &*lock {
                  println!("AV Send quit");
                  let _ = x.send(EventType::Quit);
                }
              }

              break;
            }
            _ => {}
          }
        }
        println!("Dispatcher exit");
      }).unwrap();

    ExecutorService {
      dispatcher: sender
    }
  }
}


#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn it_works() {
    let result = add(2, 2);
    assert_eq!(result, 4);
  }
}
