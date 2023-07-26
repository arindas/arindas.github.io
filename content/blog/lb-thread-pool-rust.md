+++
title = "Implementing a Load Balanced Thread Pool in Rust"
date = 2022-02-03
description = "A walk-through of a load balanced thread pool implementation from scratch."

[taxonomies]
tags = ["thread-pool", "concurrency", "rust", "priority-queue", "binary-heap"]

[extra]
toc = true
+++

![thread-pool](https://cdn.hashnode.com/res/hashnode/image/upload/v1644411476413/2aS_z0rJP.png?w=1600&h=840&fit=crop&crop=entropy&auto=compress,format&format=webp)

> In computer programming, a thread pool is a software design pattern for achieving concurrency of execution in a computer program. Often also called a replicated workers or worker-crew model, a thread pool maintains multiple threads waiting for tasks to be allocated for concurrent execution by the supervising program
>
> -- <cite>Wikipedia</cite>

Repository: <https://github.com/arindas/sangfroid>

This endeavour was largely inspired by the load balancer from the talk ["Concurrency is not parallelism"](https://youtu.be/oV9rvDllKEg) by Rob Pike.

![image.png](https://cdn.hashnode.com/res/hashnode/image/upload/v1643873997066/bbPJFKwS4.png)

Often it is useful to have a clear goal while encumbering upon a task. So what is our goal here? In the simplest sense, we want a system, which allows us to:
- Schedule some work to be done among multiple workers
- Be able to receive the result of the work done from the workers
- Ensure that the workers are sufficiently busy while also not suffering from burnout.

Notice how this kind of system is not directly tied to computers. A model like this can also be applied to any team of humans who have to accomplish some tasks.

So how do we go about modelling and solving this?

Let's say we have customers who need some work to be done. A manager which coordinates some employees, and the employees in an organisation. Here's one of the ways, work can be organised:
- The customer emails the manager what needs to be done by sending an email.
- The manager chooses the employee with the least amount of pending tasks and adds a new email in the same email chain, notifying that employee to take upon that task.
- The employee chooses the tasks from all the emails he received from his manager. Once he is done with a task, he attaches the result of the work in the same email chain and replies to the customer.
- The manager keeps note that the employee has one less task to work on.

Notice that, once the manager assigns the employee to a particular task, the customer and the employee are directly connected. The manager need not be responsible for relaying back the result of the work from the employee to the customer. This is one of the properties that we would be highly appreciable to the manager.

How does this tie into software systems you ask?

The customers can be clients who send web requests to a server. The manager is the webserver. And the workers are lightweight processes or threads. The webserver schedules web requests among the threads working which directly respond to the client with the response. With this, you have a web serving architecture.

Now, it is not feasible to keep on creating new threads with every single request, since each thread requires some memory, and involves some time for context switch from one thread to another. However, with this model, we can scale with a fixed number of threads.

Still, we haven't solved a part of the problem. How do we efficiently pick up the least loaded worker every time?

Each worker maintains a list of pending tasks it needs to solve. We then maintain a binary heap of workers, based on the lengths of their lists of pending tasks. We update this heap whenever a worker receives a new task or finishes a task.

Well, that was enough talk. Let's see some code!

### A dynamically prioritizable priority queue
That sounds a bit heavy, doesn't it?

We simply mean that we need a priority queue, where the ordering of the elements we will be using can change at runtime. Just like how our workers are prioritized for new tasks based on the number of pending tasks they have, at any given moment.

The code for this section can be found at: <https://github.com/arindas/bheap>

Let's begin!

We model our heap as follows:
```rust
/// Trait to uniquely identify elements in bheap.
pub trait Uid {
    /// Unique identifier for the implementing struct. The same value must
    /// be returned in all invocations of this method on a given struct.
    fn uid(&self) -> u64;
}

/// A re-prioritizable binary max heap containing a buffer for storing elements
/// and a hashmap index for keeping track of element positions.
pub struct BinaryMaxHeap<T>
where
    T: Ord + Uid,
{
    /// in-memory storage for elements
    buffer: Vec<T>,

    /// mapping from element uids to positions in the heap buffer
    index: HashMap<u64, usize>,
}
```
This binary heap is generic over all types that are orderable and can be identified uniquely. (Traits are mostly similar to interfaces in other languages. Read more [here](https://qr.ae/pGEbvw).)

We use a vector as our underlying storage for the binary heap. Since our elements can be dynamically ordered, we need a mechanism for keeping track of their identity and maintaining their indices.

This is what the `swap( )` function looks like:
```rust
impl<T> BinaryMaxHeap<T>
where
    T: Ord + Uid,
{
    // ...

    /// Swaps elements at the given indices by first swapping the elements
    /// in the buffer vector and next updating the `HashMap` index with
    /// new indices.
    #[inline]
    fn swap_elems_at_indices(&mut self, i: usize, j: usize) {
        let index = &mut self.index;

        index.insert(self.buffer[i].uid(), j);
        index.insert(self.buffer[j].uid(), i);

        self.buffer.swap(i, j);
    }

    // ...

}
```

It was necessary to borrow `self.index` as `mut` separately since it would otherwise require us to borrow self as `mut` more than once in the same scope.

Here's `heapify_up()`:
```rust
    /// Restores heap property by moving the element in the given index
    /// upwards along it's parents to the root, until it has no parents
    /// or it is <= to its parents.
    /// Returns Some(i) where i is the new index, None if no swaps were
    /// necessary.
    fn heapify_up(&mut self, idx: usize) -> Option<usize> {
        let mut i = idx;

        while i > 0 {
            let parent = (i - 1) / 2;

            if let Ordering::Greater = self.cmp(i, parent) {
                self.swap_elems_at_indices(i, parent);
                i = parent;
            } else {
                break;
            };
        }

        if i != idx {
            return Some(i);
        } else {
            return None;
        }
    }
```

There's nothing special about `heapify_dn()` too. You can check it out in the [repo](https://github.com/arindas/bheap/blob/main/src/lib.rs#L158). The only important thing to notice is that we use the `swap_elems_at_indices()` function every time.

Finally, our "dynamic prioritization" is here:
```rust
    // ...
    /// Restores heap property at the given position.
    pub fn restore_heap_property(&mut self, idx: usize) -> Option<usize> {
        if idx >= self.len() {
            return None;
        }

        self.heapify_up(idx).or(self.heapify_dn(idx))
    }

    /// Returns the position for element in the heap buffer.
    pub fn index_in_heap(&self, elem: &T) -> Option<usize> {
        self.index.get(&elem.uid()).map(|&elem_idx| elem_idx)
    }
} // BinaryMaxHeap
```

Now that we have the container for workers out of the way, let's implement the actual thread pool.

### The load-balanced thread-pool
Code for this section can be found at: <https://github.com/arindas/sangfroid>


#### Supporting entities
First, we need to define what a task or a `Job` is:
```rust
/// Represents a job to be submitted to the thread pool.
pub struct Job<Req, Res>
where
    Req: Send,
    Res: Send,
{
    /// Task to be executed
    task: Box<dyn FnMut(Req) -> Res + Send + 'static>,

    /// request to service i.e args for task
    req: Req,

    /// Optional result channel to send
    /// the result of the execution
    result_sink: Option<Sender<Res>>,
}
```

The Send trait boundary is necessary for types that can be moved between threads. In our case, we need the result to be sendable from the worker thread to the receiver right? Hence it's necessary.

Now let's go over the members in detail:
- `task` represent a closure that can contain some mutable state. Since its return value needs to outlive its scope, its return type is marked with `'static` lifetime, meaning its result will be alive for the entire duration of the program. It's also `Send`able. (As explained above)
- `req`: represent the parameter for the task
- `result_sink`: The sending end of the channel from which the requester can receive the result.

Hmmm, the last part seems a bit involved, doesn't it?

Whenever we create a `Job` we create a channel for communicating with the worker. Think of the channel as a pipe that has a sending end and a receiving end. Now the requester keeps the receiving end while the `Job` struct contains the sending end. When the `Job` is scheduled, the `ThreadPool` gives the job to the least loaded worker thread. The worker thread runs the closure inside the `Job` and sends back the result computed, into the sending end present in the `Job` struct. This way the worker thread was directly able to communicate the result with the requester.

Notice how the above approach, didn't require any synchronization on libraries or user's end for communicating the result. Here we share the result (or memory) by communicating, instead of communicating by synchronizing (or sharing memory) in some data structure. This is precisely what we mean when we say:
> Do not communicate by sharing memory; instead, share memory by communicating.

The rust standard library provides channels with `std::sync::mpsc::{channel, Receiver, Sender}`. Channels are created like:
```rust
let (tx, rx) = channel::<SomeType>();
```

For instance, a `Job` is constructed as follows:
```rust
impl<Req, Res> Job<Req, Res>
where
    Req: Send,
    Res: Send,
{
    // ...
    
    pub fn with_result_sink<F>(f: F, req: Req) -> (Self, Receiver<Res>)
    where
        F: FnMut(Req) -> Res + Send + 'static,
    {
        let (tx, rx) = channel::<Res>();

        (
            Job {
                task: Box::new(f),
                req,
                result_sink: Some(tx),
            },
            rx,
        )
    }
 
    // ...
}
```

Now we need a unit of communication with the worker threads. Like so:
```rust
/// Message represents a message to be sent to workers
/// in a thread pool.
pub enum Message<Req, Res>
where
    Req: Send,
    Res: Send,
{
    /// Request for job execution
    Request(Job<Req, Res>),

    /// Message the thread to terminate itself.
    Terminate,
}
```

#### Worker threads

The worker threads are represented as follows:
```rust
/// Worker represents a worker thread capable of receiving and servicing jobs.
pub struct Worker<Req, Res>
where
    Req: Send + Debug + 'static,
    Res: Send + Debug + 'static,
{
    /// uid for uniquely identifying this worker
    uid: u64,

    /// message dispatch queue
    disp_q: Sender<Message<Req, Res>>,

    /// worker thread for executing jobs
    worker: Option<JoinHandle<Result<(), WorkerError>>>,

    /// number of pending jobs to be serviced
    pending: usize,
}
```

We have simple setters for incrementing and decrementing worker load:
```rust
    /// Increments pending tasks by 1.
    #[inline]
    pub fn inc_load(&mut self) {
        self.pending += 1;
    }

    /// Decrements pending tasks by 1.
    #[inline]
    pub fn dec_load(&mut self) {
        self.pending -= 1;
    }
```

Now at the time of the creation of workers, we create a channel for dispatching messages to the worker. The sending end is part of the struct and is used by the worker manager, while the receiving end is uniquely owned by the thread servicing the jobs.

```rust
impl<Req, Res> Worker<Req, Res>
where
    Req: Send + Debug + 'static,
    Res: Send + Debug + 'static,
{
    /// Creates a new Worker from the given source, dispatch queue channel and done notice channel.
    /// The done channel and job_source channel are moved into the worker thread closure for
    /// receiving requests and notifying done status respectively.
    ///
    /// The worker expects the `mpsc::Receiver` for the done `mpsc::Sender` to outlive itself.
    pub fn new(
        job_source: Receiver<Message<Req, Res>>,
        disp_q: Sender<Message<Req, Res>>,
        done: Sender<Option<u64>>,
        uid: u64,
    ) -> Self {
        Worker {
            uid,
            disp_q,
            worker: Some(Self::worker_thread(job_source, done, uid)),
            pending: 0,
        }
    }

    /// Creates a worker thread from the given job source, done notification channel and worker uid.
    /// This is not meant to be used directly. It is advisable to construct a `Worker` instead
    /// since the `Worker` instance also manages the lifecycle and cleanup of the thread.
    /// 
    /// /// The worker thread core loop
    ///
    /// // ...
    /// while let Ok(Request(job)) = job_source.recv() {
    ///     job.result_channel.send(job.task(job.req));
    ///     done.send(worker_uid);
    /// }
    /// // ...
    /// 
    pub fn worker_thread(
        jobs: Receiver<Message<Req, Res>>,
        done: Sender<Option<u64>>,
        uid: u64,
    ) -> JoinHandle<Result<(), WorkerError>> {
        thread::spawn(move || -> Result<(), WorkerError> {
            while let Ok(Message::Request(job)) = jobs.recv() {
                job.resp_with_result()
                    .or(Err(WorkerError::ResultResponseFailed))?;

                done.send(Some(uid))
                    .or(Err(WorkerError::DoneNotificationFailed))?
            }

            Ok(())
        })
    }
```

The core of the worker thread is simply a while loop where we continuously receive messages from jobs source until no further jobs are available. For every job, we respond with the result and notify completion of a task.

Jobs may be dispatched to the thread as follows:
```rust
    /// Dispatches a job to this worker for execution.
    #[inline]
    pub fn dispatch(&self, job: Job<Req, Res>) -> Result<(), WorkerError> {
        self.disp_q
            .send(Message::Request(job))
            .or(Err(WorkerError::DispatchFailed))
    }
```

Finally, we terminate a worker thread by sending a `Terminate` message to it and `join()`-ing it.
```rust
    /// Terminates this worker by sending a Terminate message to the underlying
    /// worker thread and the invoking join() on it.
    pub fn terminate(&mut self) -> Result<(), WorkerError> {
        if self.worker.is_none() {
            return Ok(());
        }

        self.disp_q
            .send(Message::Terminate)
            .or(Err(WorkerError::TermNoticeFailed))?;

        return match self.worker.take().unwrap().join() {
            Ok(result) => result,
            Err(_) => Err(WorkerError::JoinFailed),
        };
    }
    // ...
} // Worker
```

#### ThreadPool
We represent the thread pool as follows:
```rust
/// ThreadPool to keep track of worker threads, dynamically dispatch
/// jobs in a load-balanced manner, and distribute the load evenly.
pub struct ThreadPool<Req, Res>
where
    Req: Send + Debug + 'static,
    Res: Send + Debug + 'static,
{
    pool: Option<Arc<Mutex<BinaryMaxHeap<Worker<Req, Res>>>>>,

    done_channel: Sender<Option<u64>>,

    balancer: Option<JoinHandle<Result<(), ThreadPoolError>>>,
}
```

Let's go over the members :
- `pool`: The data structure for containing our workers. We wrap it with a mutex since we use it both from the ThreadPool struct members and from the balancer thread. The `Arc` is necessary for making it `Send`-able.
- `done_channel`: The sending end of the channel to notify that a worker with a given uid has completed a task, by sending the uid of the worker in question.
- `balancer`: The balancer thread responsible for restoring the heap property once a worker completes a task.

The workers are created as follows:
```rust
    /// Creates the given number of workers and returns them in a vector along with the
    /// ends of the done channel. The workers send their Uid to the sending end of the done
    /// channel to signify completion of a job. The balancer thread receives on the
    /// receiving end of the done channel for Uid(s) and balances them accordingly.
    ///
    /// One of the key decisions behind this library is that we move channels where they
    /// are to be used instead of sharing them with a lock. The sending end of the channel
    /// is cloned and passed to each of the workers. The receiver end returned is meant
    /// to be moved to the balancer thread's closure.
    pub fn new_workers(
        workers: usize,
    ) -> (
        Vec<Worker<Req, Res>>,
        (Sender<Option<u64>>, Receiver<Option<u64>>),
    ) {
        let (done_tx, done_rx) = channel::<Option<u64>>();
        let mut worker_vec = Vec::<Worker<Req, Res>>::with_capacity(workers);

        for i in 0..workers {
            let (wtx, wrx) = channel::<Message<Req, Res>>();
            worker_vec.push(Worker::new(
                wrx,
                wtx,
                done_tx.clone(),
                i.try_into().unwrap(),
            ));
        }

        (worker_vec, (done_tx, done_rx))
    }
```

The balancer thread is created as follows:
```rust
    /// Returns a `JoinHandle` to a balancer thread for the given worker pool. The balancer
    /// listens on the given done receiver channel to receive Uids of workers who have
    /// finished their job and need to get their load decremented.
    /// The core loop of the balancer may be described as follows in pseudocode:
    /// 
    /// while uid = done_channel.recv() {
    ///     restore_worrker_pool_order(worker_pool, uid)
    /// }
    /// 
    /// Since the worker pool is shared with the main thread for dispatching jobs, we need
    /// to wrap it in a Mutex.
    pub fn balancer_thread(
        done_channel: Receiver<Option<u64>>,
        worker_heap: Arc<Mutex<BinaryMaxHeap<Worker<Req, Res>>>>,
    ) -> JoinHandle<Result<(), ThreadPoolError>> {
        thread::spawn(move || -> Result<(), ThreadPoolError> {
            while let Ok(Some(uid)) = done_channel.recv() {
                restore_worker_pool_order(
                    worker_heap
                        .lock()
                        .or(Err(ThreadPoolError::LockError))?
                        .deref_mut(),
                    uid,
                )?;
            }

            Ok(())
        })
    }
```

The logic for restoring the heap property after decrementing a workers load is as follows:
```rust
/// Restores the order of the workers in the worker pool after any modifications to the
/// number of pending tasks they have.
fn restore_worker_pool_order<Req, Res>(
    worker_pool: &mut BinaryMaxHeap<Worker<Req, Res>>,
    worker_uid: u64,
) -> Result<(), ThreadPoolError>
where
    Req: Send + Debug + 'static,
    Res: Send + Debug + 'static,
{
    if worker_pool.is_empty() {
        return Ok(());
    }

    let mut pool_restored = false;

    if let Some(i) = worker_pool.index_in_heap_from_uid(worker_uid) {
        if let Some(worker) = worker_pool.get(i) {
            worker.dec_load();
            pool_restored = true;
        }
        worker_pool.restore_heap_property(i);
    }

    return if pool_restored {
        Ok(())
    } else {
        Err(ThreadPoolError::LookupError)
    };
}

```

Hence, we create a ThreadPool with worker threads and a balancer thread as follows:
```rust
impl<Req, Res> ThreadPool<Req, Res>
where
    Req: Send + Debug + 'static,
    Res: Send + Debug + 'static,
{
    // ...
    pub fn new(workers: usize) -> Self {
        let (worker_vec, (done_tx, done_rx)) = Self::new_workers(workers);
        let worker_pool = Arc::new(Mutex::new(BinaryMaxHeap::from_vec(worker_vec)));

        let balancer = Self::balancer_thread(done_rx, Arc::clone(&worker_pool));

        ThreadPool {
            pool: Some(worker_pool),
            done_channel: done_tx,
            balancer: Some(balancer),
        }
    }
    // ...
}
```

Next, we need to be able to dispatch `Job`s to this thread pool. The idea is simple, pop the head of the heap, update its load and restore it in its correct position.
```rust
/// Schedules a new job to the given worker pool by picking up the least
/// loaded worker and dispatching the job to it.
fn worker_pool_schedule_job<Req, Res>(
    worker_pool: &mut BinaryMaxHeap<Worker<Req, Res>>,
    job: Job<Req, Res>,
) -> Result<(), ThreadPoolError>
where
    Req: Send + Debug + 'static,
    Res: Send + Debug + 'static,
{
    if worker_pool.is_empty() {
        return Err(ThreadPoolError::WorkerUnavailable);
    }

    if let Some(worker) = worker_pool.get(0) {
        worker
            .dispatch(job)
            .or(Err(ThreadPoolError::JobSchedulingFailed))?;
        worker.inc_load();
    }

    worker_pool.restore_heap_property(0);

    Ok(())
}
```

This is how it is used by the `ThreadPool` struct:
```rust
    pub fn schedule(&self, job: Job<Req, Res>) -> Result<(), ThreadPoolError> {
        if let Some(worker_pool) = &self.pool {
            worker_pool_schedule_job(
                worker_pool
                    .lock()
                    .or(Err(ThreadPoolError::LockError))?
                    .deref_mut(),
                job,
            )?;
        }

        Ok(())
    }
```

As said before, locking on the worker pool is necessary since we share it with the balancer thread.

Finally, we need a mechanism for terminating all threads in the binary heap of workers:
```rust
/// Terminates all workers in the given pool of workers by popping them
/// out and invoking `Worker::terminate()` on each of them.
fn worker_pool_terminate<Req, Res>(
    worker_pool: &mut BinaryMaxHeap<Worker<Req, Res>>,
) -> Result<(), ThreadPoolError>
where
    Req: Send + Debug + 'static,
    Res: Send + Debug + 'static,
{
    while let Some(mut worker) = worker_pool.pop() {
        worker
            .terminate()
            .or(Err(ThreadPoolError::WorkerTermFailed))?;
    }

    Ok(())
}
```

We use the above function in the `ThreadPool::terminate()`. Here it is necessary to terminate both the worker pool and the balancer thread.
```rust
    pub fn terminate(&mut self) -> Result<(), ThreadPoolError> {
        // Ensure that all threads complete their jobs and
        // complete pending done notifications if any.
        // This is necessary since the receive end of the
        // done channel is to be dropped.
        if let Some(worker_pool) = self.pool.take() {
            worker_pool_terminate(
                worker_pool
                    .lock()
                    .or(Err(ThreadPoolError::LockError))?
                    .deref_mut(),
            )?;
        }

        if self.balancer.is_none() {
            return Ok(());
        }

        self.done_channel
            .send(None)
            .or(Err(ThreadPoolError::TermNoticeFailed))?;

        return match self.balancer.take().unwrap().join() {
            Ok(result) => result,
            Err(_) => Err(ThreadPoolError::JoinFailed),
        };
    }
```

Remember that we wrapped the pool in an `Option`? It was so that we could move it to the current scope when deallocating.

We also invoke `terminate()` on `drop()`:

```rust
impl<Req, Res> Drop for ThreadPool<Req, Res>
where
    Req: Send + Debug + 'static,
    Res: Send + Debug + 'static,
{
    /// Invokes `terminate()`
    fn drop(&mut self) {
        self.terminate().unwrap()
    }
}
```

This concludes our implementation of the thread pool.

Go through the tests for `ThreadPool` [here](https://github.com/arindas/sangfroid/blob/main/src/threadpool.rs#L313).

