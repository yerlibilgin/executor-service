use std::fmt::{Display, Formatter};
use std::thread;
use std::sync::mpsc::{channel, sync_channel, Sender, SyncSender, Receiver, SendError, RecvError};
use crate::EventType::{*};
use std::sync::{Arc, Mutex, Condvar};
use log::trace;

pub type Runnable = Box<dyn Send + Sync + FnOnce() -> ()>;
pub type Callable<T> = Box<dyn Send + Sync + FnOnce() -> T>;

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum ExecutorServiceError {
  ProcessingError,
  ResultReceptionError,
}

impl<T> From<SendError<T>> for ExecutorServiceError {
  fn from(_: SendError<T>) -> Self {
    ExecutorServiceError::ProcessingError
  }
}

impl From<RecvError> for ExecutorServiceError {
  fn from(_: RecvError) -> Self {
    ExecutorServiceError::ResultReceptionError
  }
}

pub struct Future<T> {
  result_receiver: Receiver<T>,
}

impl<T> Future<T> {
  pub fn get(&self) -> Result<T, ExecutorServiceError> {
    Ok(self.result_receiver.recv()?)
  }
}

enum EventType {
  Execute(Runnable),
  ExecuteInner(Sender<EventType>, Runnable),
  Quit,
}

impl Display for EventType {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(f, "EventType::{:}",
           match self {
             Execute(_) => "Execute",
             ExecuteInner(..) => "ExecuteInner",
             Quit => "Quit",
           }
    )
  }
}

///
/// The executor service that allows tasks to be submitted/executed
/// on the underlying thread pool.
/// ```
/// use executor_service::Executors;
/// use std::thread::sleep;
/// use core::time::Duration;
///
/// let mut executor_service = Executors::new_fixed_thread_pool(2);
///
/// let some_param = "Mr White";
/// let res = executor_service.submit_sync(Box::new(move || {
///
///   sleep(Duration::from_secs(5));
///   println!("Hello {:}", some_param);
///   println!("Long lasting computation finished");
///   2
/// })).expect("Failed to submit function");
///
/// println!("Result: {:#?}", res);
/// assert_eq!(res, 2);
///```
pub struct ExecutorService {
  dispatcher: SyncSender<EventType>,
}

impl ExecutorService {
  ///
  /// Execute a function on the thread pool asynchronously with no return.
  /// ```
  /// use executor_service::Executors;
  /// use std::thread::sleep;
  /// use core::time::Duration;
  /// use std::thread;
  ///
  /// let mut executor_service = Executors::new_fixed_thread_pool(2);
  ///
  /// let some_param = "Mr White";
  /// let res = executor_service.execute(Box::new(move || {
  ///   sleep(Duration::from_secs(1));
  ///   println!("Hello from thread {:}", thread::current().name().unwrap());
  /// })).expect("Failed to execute function");
  ///
  /// sleep(Duration::from_secs(3));
  ///```
  ///
  pub fn execute(&mut self, fun: Runnable) -> Result<(), ExecutorServiceError> {
    Ok(self.dispatcher.send(Execute(fun))?)
  }

  ///
  /// Submit a function and wait for its result synchronously
  /// ```
  /// use executor_service::Executors;
  /// use std::thread::sleep;
  /// use core::time::Duration;
  ///
  /// let mut executor_service = Executors::new_fixed_thread_pool(2);
  ///
  /// let some_param = "Mr White";
  /// let res = executor_service.submit_sync(Box::new(move || {
  ///
  ///   sleep(Duration::from_secs(5));
  ///   println!("Hello {:}", some_param);
  ///   println!("Long lasting computation finished");
  ///   2
  /// })).expect("Failed to submit function");
  ///
  /// println!("Result: {:#?}", res);
  /// assert_eq!(res, 2);
  ///```
  pub fn submit_sync<T: Sync + Send + 'static>(&mut self, fun: Callable<T>) -> Result<T, ExecutorServiceError> {
    let (s, r) = sync_channel(1);
    self.dispatcher.send(Execute(Box::new(move || {
      let t = fun();
      s.send(t).unwrap();
    })))?;
    Ok(r.recv()?)
  }

  ///
  /// Submit a function and get a Future object to obtain the result
  /// asynchronously when needed.
  /// ```
  /// use executor_service::Executors;
  /// use std::thread::sleep;
  /// use core::time::Duration;
  ///
  /// let mut executor_service = Executors::new_fixed_thread_pool(2);
  ///
  /// let some_param = "Mr White";
  /// let future = executor_service.submit_async(Box::new(move || {
  ///
  ///   sleep(Duration::from_secs(3));
  ///   println!("Hello {:}", some_param);
  ///   println!("Long lasting computation finished");
  ///   "Some string result".to_string()
  /// })).expect("Failed to submit function");
  ///
  /// //Wait a bit more to see the future work.
  /// println!("Main thread wait for 5 seconds");
  /// sleep(Duration::from_secs(5));
  /// let res = future.get().expect("Couldn't get a result");
  /// println!("Result is {:}", &res);
  /// assert_eq!(&res, "Some string result");
  ///```
  pub fn submit_async<T: Sync + Send + 'static>(&mut self, fun: Callable<T>) -> Result<Future<T>, ExecutorServiceError> {
    let (s, r) = sync_channel(1);
    self.dispatcher.send(Execute(Box::new(move || {
      let t = fun();
      s.send(t).unwrap();
    })))?;

    Ok(Future {
      result_receiver: r
    })
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
    let mut guarded_count = thread_count;
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
          //trace!("{:} Waiting for job", thread::current().name().unwrap());
          let fun = r.recv().unwrap();
          //trace!("{:} Received func", thread::current().name().unwrap());
          match fun {
            ExecuteInner(sender, fun) => {
              fun();
              //trace!("{:} Send result", thread::current().name().unwrap());
              if let Ok(mut lock) = vec_clone.lock() {
                lock.push(sender);
                //trace!("Notify waiters for finish");
                cv_clone.notify_all();
              }
            }
            Quit => {
              trace!("{:} Received exit", thread::current().name().unwrap());
              break;
            }
            _ => {}
          }
        }
        //trace!("{:} Loop done", thread::current().name().unwrap())
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
                //trace!("Available: {:}", lock.len());
                let the_sender = lock.pop().unwrap();
                //trace!("Available: {:}", lock.len());
                the_sender.send(ExecuteInner(the_sender.clone(), func)).unwrap();
              };
            }
            Quit => {
              //trace!("Dispatcher received Quit");
              if let Ok(lock) = available.lock() {
                for x in &*lock {
                  trace!("AV Send quit");
                  let _ = x.send(EventType::Quit);
                }
              }

              break;
            }
            _ => {}
          }
        }
        trace!("Dispatcher exit");
      }).unwrap();

    ExecutorService {
      dispatcher: sender
    }
  }
}


#[cfg(test)]
mod tests {
  use std::time::Duration;
  use super::*;
  use std::thread::sleep;
  use std::thread;
  use std::sync::mpsc::sync_channel;
  use env_logger::{Builder, Env};
  use log::{debug, info};

  #[test]
  fn test_execute() -> Result<(), ExecutorServiceError> {
    Builder::from_env(Env::default().default_filter_or("trace")).init();
    let max = 100;
    let mut executor_service = Executors::new_fixed_thread_pool(10);

    let (sender, receiver) = sync_channel(max);
    for i in 0..max {
      let moved_i = i;

      let sender2 = sender.clone();

      executor_service.execute(Box::new(move || {
        sleep(Duration::from_millis(10));
        info!("Hello from {:} {:}", thread::current().name().unwrap(), moved_i);
        sender2.send(1).expect("Send failed");
      }))?;
    }

    let mut latch_count = max;

    loop {
      let _ = &receiver.recv().unwrap();
      latch_count -= 1;

      if latch_count == 0 {
        break; //all threads are done
      }
    };

    Ok(())
  }

  #[test]
  fn test_submit_sync() -> Result<(), ExecutorServiceError> {
    Builder::from_env(Env::default().default_filter_or("trace")).init();
    let mut executor_service = Executors::new_fixed_thread_pool(2);

    let some_param = "Mr White";
    let res = executor_service.submit_sync(Box::new(move || {
      info!("Long lasting computation");
      sleep(Duration::from_secs(5));
      debug!("Hello {:}", some_param);
      info!("Long lasting computation finished");
      2
    }))?;

    trace!("Result: {:#?}", res);
    assert_eq!(res, 2);
    Ok(())
  }

  #[test]
  fn test_submit_async() -> Result<(), ExecutorServiceError> {
    Builder::from_env(Env::default().default_filter_or("trace")).init();
    let mut executor_service = Executors::new_fixed_thread_pool(2);

    let some_param = "Mr White";
    let res: Future<String> = executor_service.submit_async(Box::new(move || {
      info!("Long lasting computation");
      sleep(Duration::from_secs(5));
      debug!("Hello {:}", some_param);
      info!("Long lasting computation finished");
      "A string as a result".to_string()
    }))?;

    //Wait a bit more to see the future work.
    info!("Main thread wait for 7 seconds");
    sleep(Duration::from_secs(7));
    info!("Main thread resumes after 7 seconds, consuming the future");
    let the_string = res.get()?;
    trace!("Result: {:#?}", &the_string);
    assert_eq!(&the_string, "A string as a result");
    Ok(())
  }
}
