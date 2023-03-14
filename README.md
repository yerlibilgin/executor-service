# executor-service

![cargo_build_workflow](https://github.com/yerlibilgin/executor-service/actions/workflows/cargobuild.yml/badge.svg)
![cargo_publish_workflow](https://github.com/yerlibilgin/executor-service/actions/workflows/cargopublish.yml/badge.svg)


A Java ExecutorService style thread pool implementation for Rust.


## Creating a thread pool
### Creating a thread pool with constant size

For many cases you might want to keep a constant sized pool to achieve
incoming tasks in parallel. Below is a sample code to achieve that:

```rust
use executor_service::Executors;
///...
fn main() {
  let mut executor_service = Executors::new_fixed_thread_pool(10).expect("Failed to create the thread pool");
  //...
}

```

### Creating a cached thread pool

If you don't want to deal with the size and want more threads as needed (e.g the threads are doing long jobs and you don't wanna wait) you can use a cached thread pool.
. Below is a sample code to achieve that:

```rust
use executor_service::Executors;
///...
fn main() {
  let mut executor_service = Executors::new_cached_thread_pool(Some(5)).expect("Failed to create the thread pool");
  //...
}

```

The `Some(5)` is an optional parameter that provides an inital size to the pool. A maximum number of 150 threads can be given. If you set it to `None`, then the pool will an initial size of `10`.

## Executing a task without waiting for the result

If you want to post a task and not mind about the result, you can use 'ExecutorService::execute' as below:

```rust
use executor_service::Executors;
use std::thread::sleep;
use core::time::Duration;
use std::thread;

fn main() {
  let mut executor_service = Executors::new_fixed_thread_pool(2).expect("Failed to create the thread pool");

  let some_param = "Mr White";
  let res = executor_service.execute(move || {
    sleep(Duration::from_secs(1));
    println!("Hello {:} from thread {:}", some_param, thread::current().name().unwrap());
  }).expect("Failed to execute function");

  sleep(Duration::from_secs(3));
}
```

## Executing a task and waiting for the result
Here is a sample, if you want to run a task on the pool and synchronously wait for the result:

```rust
use executor_service::Executors;
use std::thread::sleep;
use core::time::Duration;
fn main() {
  let mut executor_service = Executors::new_cached_thread_pool(None)
    .expect("Failed to create the thread pool");
  let some_param = "Mr White";
  let res = executor_service.submit_sync(move || {
    println!("Long computation started");
    sleep(Duration::from_secs(5));
    println!("Hello {:}", some_param);
    println!("Long computation finished");
    2
  }).expect("Failed to submit function");
  println!("Result: {:#?}", res);
  assert_eq!(res, 2);
}
```


## Executing a task and getting an asynchronous result
Here is a sample, if you want to run a task on the pool and obtain a `Future` for the result:

```rust
use executor_service::Executors;
use std::thread::sleep;
use core::time::Duration;

fn main() {
  let mut executor_service = Executors::new_cached_thread_pool(Some(5)).expect("Failed to create the thread pool");
  let some_param = "Mr White";
  let the_future = executor_service.submit_async(Box::new(move || {
    sleep(Duration::from_secs(3));
    println!("Hello {:}", some_param);
    println!("Long lasting computation finished");
    "Some string result".to_string()
  })).expect("Failed to submit function");
  
  //Wait a bit more to see the future work.
  println!("Main thread wait for 5 seconds");
  sleep(Duration::from_secs(5));
  let res = the_future.get().expect("Couldn't get a result");
  println!("Result is {:}", &res);
  assert_eq!(&res, "Some string result");
}
```

You can safely send a `Future` through a channel to allow the result to be used elsewhere


I would appreciate contributions through pull requests. Thanks!
