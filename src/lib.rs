use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::thread;
use std::sync::mpsc::{channel, sync_channel, Sender, SyncSender, Receiver, SendError, RecvError};
use std::sync::{Arc, Mutex, Condvar};
use log::trace;

pub type Runnable<T> = dyn Send + 'static + FnOnce() -> T;

///
/// Maximum number of threads that can be requested for a pool
/// This constant does not play an overarching role. In other words,
/// Of you have multiple thread pools and if the system supports,
/// you might have a total thread count of more than [MAX_THREAD_COUNT] for
/// the entire application
pub const MAX_THREAD_COUNT: u32 = 150;

///
/// Default number if thread for a cached pool
///
pub const DEFAULT_INITIAL_CACHED_THREAD_COUNT: u32 = 10;

#[derive(Debug, Clone)]
pub enum PoolType {
  Cached,
  Fixed,
}

#[derive(Debug)]
pub enum ExecutorServiceError {
  ParameterError(String),
  IOError(std::io::Error),
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

impl From<std::io::Error> for ExecutorServiceError {
  fn from(value: std::io::Error) -> Self {
    ExecutorServiceError::IOError(value)
  }
}

impl Display for ExecutorServiceError {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    match self {
      ExecutorServiceError::ParameterError(message) => write!(f, "{:}: {:}", "ParameterError", message.as_str()),
      ExecutorServiceError::IOError(io_error) => write!(f, "{:}: {:}", "IOError", io_error),
      ExecutorServiceError::ResultReceptionError => write!(f, "{:}", "ResultReceptionError"),
      ExecutorServiceError::ProcessingError => write!(f, "{:}", "ProcessingError"),
    }
  }
}

impl std::error::Error for ExecutorServiceError {}

pub struct Future<T: Send + 'static> {
  result_receiver: Receiver<T>,
}


impl<T: Send + 'static> Future<T> {
  pub fn get(&self) -> Result<T, ExecutorServiceError> {
    Ok(self.result_receiver.recv()?)
  }
}

enum DispatcherEventType<F, T>
  where F: FnOnce() -> T,
        T: Send + 'static,
        F: Send + 'static
{
  Execute(Option<SyncSender<T>>, F),
  Quit,
}

enum EventType<F, T>
  where F: FnOnce() -> T,
        T: Send + 'static,
        F: Send + 'static
{
  Execute(Option<SyncSender<T>>, Sender<Self>, F),
  Quit,
}


impl<F: Send + 'static + FnOnce() -> T, T: Send + 'static> Display for DispatcherEventType<F, T> {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(f, "EventType::{:}",
           match self {
             Self::Execute(_, _) => "Execute",
             Self::Quit => "Quit",
           }
    )
  }
}

impl<F: Send + 'static + FnOnce() -> T, T: Send + 'static> Display for EventType<F, T> {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(f, "EventType::{:}",
           match self {
             Self::Execute(_, _, _) => "Execute",
             Self::Quit => "Quit",
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
/// let mut executor_service = Executors::new_fixed_thread_pool(2).expect("Failed to create the thread pool");
///
/// let some_param = "Mr White";
/// let res = executor_service.submit_sync(move || {
///
///   sleep(Duration::from_secs(5));
///   println!("Hello {:}", some_param);
///   println!("Long computation finished");
///   2
/// }).expect("Failed to submit function");
///
/// println!("Result: {:#?}", res);
/// assert_eq!(res, 2);
///```
pub struct ExecutorService<F, T>
  where F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static {
  dispatcher: SyncSender<DispatcherEventType<F, T>>,
  pool_type: PoolType,
  thread_count: Arc<Mutex<u32>>,
}

impl<F, T> ExecutorService<F, T>
  where F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static {
  ///
  /// Execute a function on the thread pool asynchronously with no return.
  /// ```
  /// use executor_service::Executors;
  /// use std::thread::sleep;
  /// use core::time::Duration;
  /// use std::thread;
  ///
  /// let mut executor_service = Executors::new_fixed_thread_pool(2).expect("Failed to create the thread pool");
  ///
  /// let some_param = "Mr White";
  /// let res = executor_service.execute(move || {
  ///   sleep(Duration::from_secs(1));
  ///   println!("Hello {:} from thread {:}", some_param, thread::current().name().unwrap());
  /// }).expect("Failed to execute function");
  ///
  /// sleep(Duration::from_secs(3));
  ///```
  ///
  pub fn execute(&mut self, fun: F) -> Result<(), ExecutorServiceError> {
    Ok(self.dispatcher.send(DispatcherEventType::Execute(None, fun))?)
  }

  ///
  /// Submit a function and wait for its result synchronously
  /// ```
  /// use executor_service::Executors;
  /// use std::thread::sleep;
  /// use core::time::Duration;
  ///
  /// let mut executor_service = Executors::new_cached_thread_pool(None).expect("Failed to create the thread pool");
  ///
  /// let some_param = "Mr White";
  /// let res = executor_service.submit_sync(move || {
  ///
  ///   sleep(Duration::from_secs(5));
  ///   println!("Hello {:}", some_param);
  ///   println!("Long computation finished");
  ///   2
  /// }).expect("Failed to submit function");
  ///
  /// println!("Result: {:#?}", res);
  /// assert_eq!(res, 2);
  ///```
  pub fn submit_sync(&mut self, fun: F) -> Result<T, ExecutorServiceError> {
    let (s, r) = sync_channel(1);
    self.dispatcher.send(DispatcherEventType::Execute(Some(s), fun))?;
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
  /// let mut executor_service = Executors::new_cached_thread_pool(Some(5)).expect("Failed to create the thread pool");
  ///
  /// let some_param = "Mr White";
  /// let future = executor_service.submit_async(Box::new(move || {
  ///
  ///   sleep(Duration::from_secs(3));
  ///   println!("Hello {:}", some_param);
  ///   println!("Long computation finished");
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
  pub fn submit_async(&mut self, fun: F) -> Result<Future<T>, ExecutorServiceError>
  {
    let (s, r) = sync_channel(1);
    self.dispatcher.send(DispatcherEventType::Execute(Some(s), fun))?;

    Ok(Future {
      result_receiver: r
    })
  }

  pub fn pool_type(&self) -> &PoolType {
    &self.pool_type
  }


  pub fn get_thread_count(&self) -> Result<u32, ExecutorServiceError> {
    match self.thread_count.lock() {
      Ok(lock) => Ok(*lock),
      Err(_) => Err(ExecutorServiceError::ProcessingError)
    }
  }
}

impl<F, T> Drop for ExecutorService<F, T>
  where F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static {
  fn drop(&mut self) {
    self.dispatcher.send(DispatcherEventType::Quit).unwrap();
  }
}


pub struct Executors<F, T> where F: FnOnce() -> T,
                                 F: Send + 'static,
                                 T: Send + 'static {
  _phantom: PhantomData<F>,
}

impl<F, T> Executors<F, T> where F: FnOnce() -> T,
                                 F: Send + 'static,
                                 T: Send + 'static {
  ///
  /// Creates a thread pool with a fixed size. All threads are initialized at first.
  ///
  /// `REMARKS`: The maximum value for [thread_count] is currently [MAX_THREAD_COUNT]
  /// If you go beyond that, the function will fail, producing an [ExecutorServiceError::ParameterError]
  ///
  pub fn new_fixed_thread_pool(thread_count: u32) -> Result<ExecutorService<F, T>, ExecutorServiceError> {
    if thread_count > MAX_THREAD_COUNT {
      return Err(ExecutorServiceError::ProcessingError);
    }

    let thread_count_mutex = Arc::new(Mutex::new(thread_count));
    let pool_type = PoolType::Fixed;
    let sender = Self::prepare_pool(thread_count, pool_type.clone(), thread_count_mutex.clone())?;

    Ok(ExecutorService {
      dispatcher: sender,
      pool_type,
      thread_count: thread_count_mutex,
    })
  }


  ///
  /// Creates a cached thread pool with an optional initial thread count. If the initial
  /// count is not provided, then a default of [DEFAULT_INITIAL_CACHED_THREAD_COUNT] threads will be initiated. When a new
  /// task is posted to the pool, if there are no threads available, then a new thread
  /// will be added to the pool and will then be cached. So the number of underlying
  /// threads is likely to increase with respect to the needs.
  ///
  /// `REMARKS`: The maximum value for `initial_thread_count` is currently [MAX_THREAD_COUNT]. And
  /// the maximum number of thread that can be created is also limited to [MAX_THREAD_COUNT] by design.
  /// If more requests come and all threads are busy and we have a maximum of [MAX_THREAD_COUNT] threads,
  /// then it will behave like a constant thread pool.
  ///
  pub fn new_cached_thread_pool(initial_thread_count: Option<u32>) -> Result<ExecutorService<F, T>, ExecutorServiceError> {
    let initial_count = if let Some(count) = initial_thread_count {
      if count > MAX_THREAD_COUNT {
        return Err(ExecutorServiceError::ParameterError(format!("Max thread count is {:}", MAX_THREAD_COUNT)));
      }
      count
    } else {
      DEFAULT_INITIAL_CACHED_THREAD_COUNT
    };


    let pool_type = PoolType::Cached;
    let thread_count_mutex = Arc::new(Mutex::new(initial_count));

    let sender = Self::prepare_pool(initial_count, pool_type.clone(), thread_count_mutex.clone())?;

    Ok(ExecutorService {
      dispatcher: sender,
      pool_type,
      thread_count: thread_count_mutex,
    })
  }

  fn prepare_pool(initial_count: u32, pool_type: PoolType, thread_count_mutex: Arc<Mutex<u32>>)
                  -> Result<SyncSender<DispatcherEventType<F, T>>, ExecutorServiceError> {
    let available = Arc::new(Mutex::new(vec![]));

    let (sender, receiver) = sync_channel(1);

    let pool_waiter = Arc::new(Condvar::new());

    for i in 0..initial_count {
      let (s, r) = channel();

      if let Ok(mut lock) = available.lock() {
        lock.push(s);
      }

      Self::create_thread(i, r, available.clone(), pool_waiter.clone())?;
    }

    Self::prepare_dispatcher(available, receiver, pool_waiter, pool_type, initial_count, thread_count_mutex)?;
    Ok(sender)
  }

  fn create_thread(i: u32, r: Receiver<EventType<F, T>>,
                   vec_clone: Arc<Mutex<Vec<Sender<EventType<F, T>>>>>,
                   cv_clone: Arc<Condvar>)
                   -> Result<(), ExecutorServiceError> {
    thread::Builder::new()
      .name(format!("Thread-{:}", i)).spawn(move || {
      loop {
        let fun = r.recv().unwrap();
        match fun {
          EventType::Execute(result_sender, sender, fun) => {
            let t = fun();
            if let Some(res_sender) = result_sender {
              //TODO: Check the result
              res_sender.send(t).unwrap();
            }
            if let Ok(mut lock) = vec_clone.lock() {
              lock.push(sender);
              cv_clone.notify_all();
            }
          }
          EventType::Quit => {
            trace!("{:} Received exit", thread::current().name().unwrap());
            break;
          }
        }
      }
      //trace!("{:} Loop done", thread::current().name().unwrap())
    })?;

    Ok(())
  }

  fn prepare_dispatcher(available: Arc<Mutex<Vec<Sender<EventType<F, T>>>>>,
                        receiver: Receiver<DispatcherEventType<F, T>>,
                        pool_waiter: Arc<Condvar>,
                        pool_type: PoolType,
                        current_thread_count: u32,
                        thread_count_mutex: Arc<Mutex<u32>>) -> Result<(), ExecutorServiceError> {
    thread::Builder::new()
      .name("Dispatcher".into())
      .spawn(move || {
        //shadowing deliberately
        let mut current_thread_count = current_thread_count;
        loop {
          match receiver.recv().unwrap() {
            DispatcherEventType::Execute(result_sender, func) => {
              if let Ok(mut lock) = available.lock() {
                if lock.is_empty() {
                  //threads are busy
                  match pool_type {
                    PoolType::Cached => {
                      //the pool is cached.
                      if current_thread_count < MAX_THREAD_COUNT {
                        //spawn a new thread
                        let (s, r) = channel::<EventType<F, T>>();
                        lock.push(s);
                        //FIXME: use this result!
                        Self::create_thread(current_thread_count, r, available.clone(), pool_waiter.clone());
                        current_thread_count += 1;
                        let mut count_lock = thread_count_mutex.lock().unwrap();
                        *count_lock = current_thread_count;
                      } else {
                        //we already have a maximum, so wait again
                        let _ = pool_waiter.wait(lock);
                      }
                    }
                    PoolType::Fixed => {
                      //the pool is fixed, we have to wait.
                      let _ = pool_waiter.wait(lock);
                    }
                  }
                }
              };

              if let Ok(mut lock) = available.lock() {
                //trace!("Available: {:}", lock.len());
                let the_sender = lock.pop().unwrap();
                //trace!("Available: {:}", lock.len());
                the_sender.send(EventType::Execute(result_sender, the_sender.clone(), func)).unwrap();
              };
            }
            DispatcherEventType::Quit => {
              //trace!("Dispatcher received Quit");
              if let Ok(lock) = available.lock() {
                for x in &*lock {
                  trace!("AV Send quit");
                  let _ = x.send(EventType::Quit);
                }
              }

              break;
            }
          }
        }
        trace!("Dispatcher exit");
      })?;

    Ok(())
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

  #[cfg(test)]
  #[ctor::ctor]
  fn init_env_logger() {
    Builder::from_env(Env::default().default_filter_or("trace")).init();
  }

  #[test]
  fn test_execute() -> Result<(), ExecutorServiceError> {
    let max = 100;
    let mut executor_service = Executors::new_fixed_thread_pool(10)?;

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
    let mut executor_service = Executors::new_fixed_thread_pool(2)?;

    let some_param = "Mr White";
    let res = executor_service.submit_sync(Box::new(move || {
      info!("Long computation");
      sleep(Duration::from_secs(5));
      debug!("Hello {:}", some_param);
      info!("Long computation finished");
      2
    }))?;

    trace!("Result: {:#?}", res);
    assert_eq!(res, 2);
    Ok(())
  }

  #[test]
  fn test_submit_async() -> Result<(), ExecutorServiceError> {
    let mut executor_service = Executors::new_fixed_thread_pool(2)?;

    let some_param = "Mr White";
    let res: Future<String> = executor_service.submit_async(Box::new(move || {
      info!("Long computation");
      sleep(Duration::from_secs(5));
      debug!("Hello {:}", some_param);
      info!("Long computation finished");
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

  #[test]
  fn test_cahced_thread_pool_execute() -> Result<(), ExecutorServiceError> {
    let mut executor_service = Executors::new_cached_thread_pool(None)?;

    let (s, r) = channel();
    let some_param = "Mr White";

    for _ in 0..100 {
      let s = s.clone();
      sleep(Duration::from_millis(100));
      debug!("Thread count is {:}", executor_service.get_thread_count().unwrap());
      executor_service.execute(move || {
        info!("Long computation Thread:{:}", thread::current().name().unwrap());
        sleep(Duration::from_millis(15000));
        debug!("Hello {:}", some_param);
        info!("Long computation finished");
        s.send("asdf").expect("Cannot send");
      })?;
    }

    for _ in 0..100 {
      r.recv().expect("Cannot receive");
    }

    Ok(())
  }


  #[test]
  fn test_submit_async_cached() -> Result<(), ExecutorServiceError> {
    let mut executor_service = Executors::new_cached_thread_pool(Some(5))?;

    info!("{:?}", executor_service.get_thread_count());
    let some_param = "Mr White";
    let the_future: Future<String> = executor_service.submit_async(Box::new(move || {
      info!("Long computation");
      sleep(Duration::from_secs(5));
      debug!("Hello {:}", some_param);
      info!("Long computation finished");
      "A string as a result".to_string()
    }))?;

    //Wait a bit more to see the future work.
    info!("Main thread wait for 7 seconds");
    sleep(Duration::from_secs(7));
    info!("Main thread resumes after 7 seconds, consuming the future");


    thread::spawn(move || {
      let the_string = the_future.get().expect("No result");
      trace!("Result: {:#?}", &the_string);
      assert_eq!(&the_string, "A string as a result");
    }).join().expect("Join failed");
    Ok(())
  }
}
