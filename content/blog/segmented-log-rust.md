+++
title = "Building Segmented Logs in Rust: From Theory to Production!"
date = 2023-08-01
description = "Explore a Rust implementation of the persistence mechanism behind message-queues and write-ahead-logs in databases. Embark on a journey from the original segmented log research paper to a production grade, fully tested and benchmarked implementation."
draft = true

[taxonomies]
tags = ["rust", "tokio", "glommio", "segmented-log", "message-queue", "distributed-systems"]

[extra]
toc = true
giscus = true
quick_navigation_buttons = true
header_image = "/img/segmented-log-basic-intro.png"
+++

>_Psst._ Do you already know segmented logs well? If yes, jump [here](#a-segmented-log-implementation).

## Prologue: The Log 📃🪵

First, let's clarify what we mean by a "log" in this context. Here, log refers
to an append only ordered collection of records where the records are ordered
by time.

<p align="center">
<img src="/img/log.png" alt="queue-diagram" width="50%"/>
</p>
<p align="center" class="caption">
<b>Fig:</b> The Log: An append only ordered collection of records.
</p>

Since the records are ordered by time, the log's record indices can be thought
of as timestamps, with the convenient property of being decoupled from actual
wall-clock time.

The log indices effectively behave as Lamport clocks[^1].

So, why do we care about these kinds of logs? Why are they useful?

### Log Usage: An example scenario

There's a teacher and a set of students in a classroom. The teacher wants to
hold an elementary arithmetic lesson.

The teacher makes every student write down a particular initial number. (e.g
42). Next, the teacher plans to give a sequence of instructions to the
students. The teacher may give the following kinds of instructions:
```
- Add x; where x is a number
- Sub x; where x is a number
- Mul x; where x is a number
- Div x; where x is a non-zero number
```

For every instruction, the students are to apply the instruction to their
current number, calculate the new number, and write down the new number as
their current number. The students are to continue this process for every
instruction, till the teacher finishes giving instructions. They are then
required to submit the final calculated number to the teacher.

<p align="center">
<img src="/img/log-less-direct-repl.png" alt="log-less-direct-repl"/>
</p>
<p align="center" class="caption">
<b>Fig:</b> Sample scenario with initial number 42.
</p>

>So for instance, if the current number is 42, and the received instruction is
>`Add 3`, the student calculates:
>
>```
>result = Add 3 (current_number) = current_number + 3 = 42 + 3 = 45
>current_number = result ;; 45
>```
>
>Next if the student receives, `Div 5`:
>
>```
>result = Div 5 (current_number) = current_number / 5 = 45 / 5 = 9
>current_number = result ;; 9
>```

Now, if all students start from the same initial number, and apply the same
sequence of operations on their number, they are bound to come to the same
result! So the teacher also gives the students a self grading lesson by telling
the final number at the end of the class. If the student got the same final
number, they scored full marks.

Notice that the students must follow the following to get full marks:
- Start from the same correct initial number
- Apply all the operations correctly in the given _order_, _without random
  mistakes_ in the correct _pre-determined_ way.


With computer science, we can model the students as _deterministic_ state
machines, the students' `current_number` as their internal state, and the
instructions as inputs to the state machines. From here, we can say the
following:

---

If different identical deterministic state machines start from the same initial
state, and receive the same set of inputs in the same order, they will end in
the same state.

---

We call this the _state machine replication_ principle.

#### State machine replication

Now there's a new problem: the last two rows of students can't hear the teacher
properly. What do they do now? The teacher needs a solution that enables them
to give the same set of instructions to the backbenchers without actually
giving them the answers.

The solution here is to give the instructions in writing. However, in an
exercise to foster teamwork between their students, the teacher delegates the
instruction sharing task to the students.

<p align="center">
<img alt="log-based-repl" src="/img/log-based-repl.png" />
</p>
<p align="center" class="caption">
<b>Fig:</b> Log based state machine replication between front, middle and back benchers.
</p>

The students come up with the following solution:

They first write down the instructions sequentially on sheets of paper, and
then perform the calculations separately on their own private sheet. When they
are done writing on a sheet of paper with the instructions, they only share the
sheet containing the instructions. They never share their private sheet. After
passing a sheet full of instructions, they start writing the subsequent
instructions on a new sheet of paper.

The backbenchers are able to receive the same set of instructions in the same
sequence through the sheets. They perform the necessary calculations on their
own private sheet starting from the same initial number using the instructions
from the received sheets.

If we inspect carefully, this mechanism of sharing the instructions through the
sheets behaves like a log. The private sheets act as the internal states. The
collection of sheets collectively act as a log.

Now, in our case, because the backbenchers receive the same set of instructions
in the same sequence, they go through the same set of internal states in the
same sequence and arrive at the same final state. They effectively replicate
the front and middle-benchers. Since, we can model students as state machines,
we effectively did __state machine replication__ with a __log__.

Finally, since the backbenchers receive the instructions through the log and
not directly from the teacher, they lag behind a bit but eventually arrive at
the same results. So we can say there is a __replication lag__.

>These concepts directly translate to distributed systems. Consider this:
>
>There is a database partition in a distributed database responsible for a
>certain subset of data. When any data manipulation queries are routed to it, it
>has to handle the queries. Instead of directly committing the effect of the
>queries on the underlying storage, it first writes the operations to be applied
>on the local storage, one-by-one into a log called the "write-ahead-log". Then
>it applies the operations from the write ahead log to the local storage.
>
>In case there is a database failure, it can re-apply the operations in the
>write-ahead-log from the last committed entry and arrive at the same state.
>
>When this database partition needs to replicate itself to other follower
>partitions, it simply replicates the write-ahead-log to the followers instead
>of copying over the entire materialized state. The followers can use the same
>write ahead log to arrive to the same state as the leader partition.
>
>Now the follower partitions, have to receive the write-ahead-log first over the
>network. Only then can they apply the operations. As a result they lag behind
>the leader. This is replication lag.

#### Asynchronous processing

Now in the same classroom scenario, what happens when a student is absent?
Turns out they still need to do the assignment.

The backbenchers come to the rescue! They share the ordered sheets of
instructions and the initial number with the student in distress. The student
gleefully applies the instructions on the number, arrives at the same results
and scores full marks.

However, notice what happened here: The student did the assignment in a
completely out of sync or __asynchronous__ way with respect to the other
students.

---

Logs enable asynchronous processing of requests.

---

Message queues provide a convenient abstraction over logs to enable
asynchronous processing. A server might not be able to synchronously handle all
requests due to lack of resources. So instead, it can buffer the requests in a
message queue. A different server can then pick up the requests one-by-one and
handle them.

Now it's not necessary that all the requests have to be handled by the same
server. Because the log is shared, different servers may choose to share the
load with each other. In this case, the requests are distributed between the
servers to load-balance the requests. (Provided that there is no causal
dependency between the requests.)

For instance, a simple scheme might be: If there are N servers, the server for
a particular request is decided with `request.index % N`.

If you want to read more about the usage of logs in distributed systems, read
Jay Krep's (co-creator of Apache Kafka) excellent blog post on this topic
[here](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying).

## Segmented Log 🪚🪵

It might come as a surpise, but we have already come across a _segmented log_
in the previous example.

### Introduction

In the previous example, the collection of sheets containing the instructions
collectively behaved as a log. We can also say that the _instruction log_ was
_segmented_ across the different sheets of paper.

Call the individual sheets of paper _segments_. The collection of sheets can
now be called a _segmented log_.

Wait wait, don't leave yet! 😅 Let's take this a bit more seriously this time.

Let's go back to the log. At the end of the day a log is sequential collection
of elements. What's the simplest data structure we can use to implement this?

An array. 

However, we need __persistence__. So let's use a file based abstraction
instead. 

>We can quite literally map a file to a process's virtual memory
>address space using the
>[`mmap()`](https://man7.org/linux/man-pages/man2/mmap.2.html) system call and
>then use it like an array, but that's a topic for a different day.

<p align="center">
<img src="/img/log.png" alt="queue-diagram" width="50%"/>
</p>
<p align="center" class="caption">
<b>Fig:</b> A log implementation based on a file.
</p>

Since our file based abstraction needs to support an append only
data-structure, it internally sequentially writes to the end of the internal
file. Assume that this abstraction allows you to uniquely refer to any entry
using it's index.

Now, this setup has some problems:
- All entries are sequentially written to a single large file
- A single large file is difficult to store, move and copy
- Few bad sectors in the underlying disk can make the whole file unrecoverable.
This can render all stored data unusable.

The logical next step is to split this abstraction across multiple smaller
units. We call these smaller units _segments_.

<p align="center">
<img src="/img/segmented-log-basic-intro.png" alt="segmented-log-basic-intro"/>
</p>
<p align="center" class="caption">
<b>Fig:</b> <code>segmented_log</code> outline.
</p>

In this solution:
- The record index range is split across smaller units called _segments_. The
  index ranges of different _segments_ are non-overlapping.
- Each _segment_ individually behaves like a log
- For each _segment_ we maintain an entry: `segment`. This `segment` entry
  stores the index range serviced by it along with a handle to the underlying
  file
- We keep the `segment` entries sorted by their starting index
- The first n - 1 _segments_  are called _read segments_. Their `segment`
  entries are stored in a vector called `read_segments`
- The last _segment_ is called the _write segment_. We assign it's `segment`
  entry to `write_segment`.

Write behaviour:
- All writes go to the `write_segment`
- Each `segment` has a threshold on it's size
- When the `write_segment` size exceeds it's threshold:
  - We close the `write_segment`
  - We reopen it as a _read segment_.
  - We push back the newly opened _read segment_ `segment` entry to the vector
    `read_segments`.
  - We create a new `segment` with it index range starting after the end of the
    previous _write segment_. This `segment` is assigned to `write_segment`
  
Read behaviour (for reading a record at particular index):
- Locate the `segment` where the index falls within the `segment`'s index
  range. Look first in the `read_segments` vector, fall back to `write_segment`
- Read the record at the given index from the located `segment`


### Original description in the Apache Kafka paper

This section presents the `segmented_log` as described in the Apache Kafka
[paper](https://pages.cs.wisc.edu/~akella/CS744/F17/838-CloudPapers/Kafka.pdf).

<p align="center">
<img src="/img/kafka-segmented-log.png" alt="kafka-segmented-log"/>
</p>
<p align="center" class="caption">
<b>Fig:</b> <code>segmented_log</code> <i>(Fig. 2)</i> from the the Apache Kafka paper.
</p>

>__Simple storage__: Kafka has a very simple storage layout. Each
>partition of a topic corresponds to a logical log. Physically, a log
>is implemented as a set of segment files of approximately the
>same size (e.g., 1GB). Every time a producer publishes a message
>to a partition, the broker simply appends the message to the last
>segment file. For better performance, we flush the segment files to
>disk only after a configurable number of messages have been
>published or a certain amount of time has elapsed. A message is
>only exposed to the consumers after it is flushed.
>
>Unlike typical messaging systems, a message stored in Kafka
>doesn’t have an explicit message id. Instead, each message is
>addressed by its logical offset in the log. This avoids the overhead
>of maintaining auxiliary, seek-intensive random-access index
>structures that map the message ids to the actual message
>locations. Note that our message ids are increasing but not
>consecutive. To compute the id of the next message, we have to
>add the length of the current message to its id. From now on, we
>will use message ids and offsets interchangeably.
>
>A consumer always consumes messages from a particular
>partition sequentially. If the consumer acknowledges a particular
>message offset, it implies that the consumer has received all
>messages prior to that offset in the partition. Under the covers, the
>consumer is issuing asynchronous pull requests to the broker to
>have a buffer of data ready for the application to consume. Each
>pull request contains the offset of the message from which the
>consumption begins and an acceptable number of bytes to fetch.
>Each broker keeps in memory a sorted list of offsets, including the
>offset of the first message in every segment file. The broker
>locates the segment file where the requested message resides by
>searching the offset list, and sends the data back to the consumer.
>After a consumer receives a message, it computes the offset of the
>next message to consume and uses it in the next pull request.
>
>The layout of an Kafka log and the in-memory index is depicted in
>Figure 2. Each box shows the offset of a message.

The main difference here is that instead of referring to records with a simple
index, we refer to it with a logical offset. This is important because the
offset is dependent on the record sizes. The offset for next record has to be
calculated as the sum of current record offset and current record size.

## A `segmented_log` implementation

The code for this section is in this repository: <https://github.com/arindas/laminarmq/>
More specifically, in the [storage module](https://github.com/arindas/laminarmq/tree/main/src/storage).

### Implementation outline

<p align="center">
<img src="https://raw.githubusercontent.com/arindas/laminarmq/assets/assets/diagrams/laminarmq-segmented-log.svg" alt="segmented_log"/>
</p>
<p align="center">
<b>Fig:</b> Data organisation for persisting a <code>segmented_log</code> on a <code>*nix</code> file system.
</p>

A segmented log is a collection of read segments and a single write segment.
Each "segment" is backed by a storage file on disk called "store". The offset
of the first record in a segment is the `base_offset`.

The log is:
- "immutable", since only "append", "read" and "truncate" operations are
  allowed. It is not possible to update or delete records from the middle of
  the log.
- "segmented", since it is composed of segments, where each segment services
  records from a particular range of offsets.

All writes go to the write segment. A new record is written at `write_segment.next_offset`.

When we max out the capacity of the write segment, we close the write segment
and reopen it as a read segment. The re-opened segment is added to the list of
read segments. A new write segment is then created with `base_offset` equal to
the `next_offset` of the previous write segment.

When reading from a particular offset, we linearly check which segment contains
the given read segment. If a segment capable of servicing a read from the given
offset is found, we read from that segment. If no such segment is found among
the read segments, we default to the write segment. The following scenarios may
occur when reading from the write segment in this case:
- The write segment has synced the messages including the message at the given
  offset. In this case the record is read successfully and returned.
- The write segment hasn't synced the data at the given offset. In this case
  the read fails with a segment I/O error.
- If the offset is out of bounds of even the write segment, we return an "out
  of bounds" error.

### Enhancements to the design to enable streaming writes

While the conventional `segmented_log` data structure is quite performant for a
`commit_log` implementation, it still requires the following properties to hold
true for the record being appended:
- We have the entire record in memory
- We know the record bytes' length and record bytes' checksum before the record
  is appended

It's not possible to know this information when the record bytes are read from
an asynchronous stream of bytes. Without the enhancements, we would have to
concatenate intermediate byte buffers to a vector. This would not only incur
more allocations, but also slow down our system.

Hence, to accommodate this use case, we introduced an intermediate indexing
layer to our design.

<p align="center">
<img src="https://raw.githubusercontent.com/arindas/laminarmq/assets/assets/diagrams/laminarmq-indexed-segmented-log-landscape.svg" alt="segmented_log" />
</p>

```
//! Index and position invariants across segmented_log

// segmented_log index invariants
segmented_log.lowest_index  = segmented_log.read_segments[0].lowest_index
segmented_log.highest_index = segmented_log.write_segment.highest_index

// record position invariants in store
records[i+1].position = records[i].position + records[i].record_header.length

// segment index invariants in segmented_log
segments[i+1].base_index = segments[i].highest_index
                         = segments[i].index[index.len-1].index + 1
```
<p align="center">
<b>Fig:</b> Data organisation for persisting a <code>segmented_log</code> on a
<code>*nix</code> file system.
</p>

In the new design, instead of referring to records with a raw offset, we refer
to them with indices. The index in each segment translates the record indices
to raw file position in the segment store file.

Now, the store append operation accepts an asynchronous stream of bytes instead
of a contiguously laid out slice of bytes. We use this operation to write the
record bytes, and at the time of writing the record bytes, we calculate the
record bytes' length and checksum. Once we are done writing the record bytes to
the store, we write it's corresponding `record_header` (containing the checksum
and length), position and index as an `index_record` in the segment index.

This provides two quality of life enhancements:
- Allow asynchronous streaming writes, without having to concatenate
  intermediate byte buffers
- Records are accessed much more easily with easy to use indices

Now, to prevent a malicious user from overloading our storage capacity and
memory with a maliciously crafted request which infinitely loops over some data
and sends it to our server, we have provided an optional `append_threshold`
parameter to all append operations. When provided, it prevents streaming append
writes to write more bytes than the provided `append_threshold`.

At the segment level, this requires us to keep a segment overflow capacity. All
segment append operations now use `segment_capacity - segment.size +
segment_overflow_capacity` as the `append_threshold` value. A good
`segment_overflow_capacity` value could be `segment_capacity / 2`.

### Component implementation

We now proceed in a bottom-up fashion to implement the entirety of an "indexed
segmented log".

#### `AsyncIndexedRead` (_trait_)

If we notice carefully, there is a common thread between `Index`, `Segment` and
even `SegmentedLog` as a whole. Even though they are at different levels in the
compositional hierarchy, the share some similar _traits_:
- They allow reading items from specific logical indices
- They have a notion of highest index and lowest index
- The read operation has to be asynchronous in nature to support both in-memory
  and on-disk storage mechanisms.

Let's formalize these notions:

```rust
/// Collection providing asynchronous read access to an indexed set of records (or 
/// values).
#[async_trait(?Send)]
pub trait AsyncIndexedRead {
    /// Error that can occur during a read operation.
    type ReadError: std::error::Error;

    /// Value to be read.
    type Value;

    /// Type to index with.
    type Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy;

    /// Reads the value at the given index.
    async fn read(&self, idx: &Self::Idx) -> Result<Self::Value, Self::ReadError>;

    /// Index upper exclusive bound
    fn highest_index(&self) -> Self::Idx;

    /// Index lower inclusive bound
    fn lowest_index(&self) -> Self::Idx;

    /// Returns whether the given index is within the index bounds of this
    /// collection.
    ///
    /// This method checks the following condition:
    /// `lowest_index <= idx < highest_index`
    fn has_index(&self, idx: &Self::Idx) -> bool {
        *idx >= self.lowest_index() && *idx < self.highest_index()
    }

    /// Returns the number of values in this collection.
    fn len(&self) -> Self::Idx {
        self.highest_index()
            .checked_sub(&self.lowest_index())
            .unwrap_or(num::zero())
    }

    /// Returns whether this collection is empty.
    fn is_empty(&self) -> bool {
        self.len() == num::zero()
    }

    /// Normalizes the given index between `[0, len)` by subtracting 
    /// `lowest_index` from it.
    ///
    /// Returns `Some(normalized_index)` if the index is within 
    /// bounds, `None` otherwise.
    fn normalize_index(&self, idx: &Self::Idx) -> Option<Self::Idx> {
        self.has_index(idx)
            .then_some(idx)
            .and_then(|idx| idx.checked_sub(&self.lowest_index()))
    }
}
```

#### `AsyncTruncate` (_trait_)

Now each of our components need to support a "truncate" operation, where
everything sequentially after a certain "mark" is removed. This notion
can be expressed as a _trait_:

```rust
/// Trait representing a truncable collection of records, which can be truncated after a
// "mark".
#[async_trait(?Send)]
pub trait AsyncTruncate {
    /// Error that can occur during a truncation operation.
    type TruncError: std::error::Error;

    /// Type to denote a truncation "mark", after which the collection will be truncated.
    type Mark: Unsigned;

    /// Truncates this collection after the given mark, such that this collection
    /// contains records only upto this "mark". (item at "mark" is excluded)
    async fn truncate(&mut self, mark: &Self::Mark) -> Result<(), Self::TruncError>;
}
```

#### `AsyncConsume` (_trait_)

Next, every abstraction related to storage needs to be safely closed to persist
data, or be removed all together. We call such operations "consume" operations.
As usual, we codify this general notion with a _trait_:

```rust
/// Trait representing a collection that can be closed or removed entirely.
#[async_trait(?Send)]
pub trait AsyncConsume {
    /// Error that can occur during a consumption operation.
    type ConsumeError: std::error::Error;

    /// Removes all storage associated with this collection.
    ///
    /// The records in this collection are completely removed.
    async fn remove(self) -> Result<(), Self::ConsumeError>;

    /// Closes this collection.
    ///
    /// One would need to re-qcquire a handle to this collection from the storage
    /// in-order to access the records ot this collection again.
    async fn close(self) -> Result<(), Self::ConsumeError>;
}
```

#### `Sizable` (_trait_)

Every entity capable of storing data needs a mechanism for measuring it's
storage footprint i.e size in number of bytes.

```rust
/// Trait representing collections which have a measurable size in number of bytes.
pub trait Sizable {
    /// Type to represent the size of this collection in number of bytes.
    type Size: Unsigned + FromPrimitive + Sum + Ord;

    /// Returns the size of this collection in butes.
    fn size(&self) -> Self::Size;
}
```

#### `Storage` (_trait_)

Finally, we need a mechanism to read and persist data. This mechanism needs to
support reads at random positions and appends to the end.

"But Arindam!", you interject. "Such a mechanism exists! It's called a file.",
you say triumphantly. And you wouldn't be wrong. However we have the following
additional requirements:
- It has to be cross platorm and independent of async rumtimes.
- It needs to provide a simple API for random reads without having to seek some
  pointer.
- It needs to support appending a stream of byte slices.

Alright, let's begin:

```rust
#[derive(Debug)]
pub struct StreamUnexpectedLength;

// ... impl Display for StreamUnexpectedLength ...

impl std::error::Error for StreamUnexpectedLength {}

/// Trait representing a read-append-truncate storage media.
#[async_trait(?Send)]
pub trait Storage:
    AsyncTruncate<Mark = Self::Position, TruncError = Self::Error>
    + AsyncConsume<ConsumeError = Self::Error>
    + Sizable<Size = Self::Position>
{
    /// Type to represent the content bytes of this storage media.
    type Content: Deref<Target = [u8]> + Unpin;

    /// Type to represent data positions inside this storage media.
    type Position: Unsigned + FromPrimitive + ToPrimitive + Sum + Ord + Copy;

    /// Error that can occur during storage operations.
    type Error: std::error::Error + From<StreamUnexpectedLength>;

    /// Appends the given slice of bytes to the end of this storage.
    ///
    /// Returns the position at which the slice was written, and the number
    /// of bytes written.
    async fn append_slice(
        &mut self,
        slice: &[u8],
    ) -> Result<(Self::Position, Self::Size), Self::Error>;

    /// Reads `size` number of bytes from the given `position`.
    ///
    /// Returns the bytes read.
    async fn read(
        &self,
        position: &Self::Position,
        size: &Self::Size,
    ) -> Result<Self::Content, Self::Error>;

} // ...where's streaming append?

```

First, let's unpack what's going on here:
- We support a notion of `Content`, the slice of bytes that is read
- We also have a notion for `Position` to identify the position of reads and
  appends.
- First, we have a simple `append_slice()` API the simply appends a given slice
of bytes to the end of the storage. It returns the position at which slice was
written, along with the number of bytes written.
- Next, we have a `read()` API for reading a slice of bytes of a particular
  `size` from a particular `position`. It returns a `Content`, the associated
  type used to represent slice of bytes that are read from this storage.
- Every operation here is fallible. So errors and `Result`(s) are natural.
  However, what's up with `StreamUnexpectedLength`? Keep that in mind for now.
- Our storage can be truncated. We inherit from `AsyncTruncate`. We treat
  `Position` as the truncation mark.
- Our storage is also consumable. We inherit from `AsyncConsume` for `close()`
  and `remove()`
- Finally, our storage has a notion of size with `Sizable`. We use our `Position`
type for representing sizes.

Now, we need to support streaming appends with the existing methods.

Let's begin by asking ourselves, what exactly are the arguments here?

A stream of byte slice. How do we represent that?

Let's start with just a slice. Let's call it `XBuf`. The bound for that is
simple enough:

```rust
XBuf: Deref<Target = [u8]>
```

Now we need a stream of these. Also note that we need the reading of a single
item from the stream to also be fallible.

First let's just consider a stream of `XBuf`. Let's call the stream `X`:
```rust
X: Stream<Item = XBuf>
```

Now, to let's consider an error type `XE`. To make every read from the stream
fallible, `X` needs the following bounds:

```rust
X: Stream<Item = Result<XBuf, XE>>
```

Now our stream needs to be `Unpin` so that it we can safely take a `&mut`
reference to it in our function.

>This blog post: <https://blog.cloudflare.com/pin-and-unpin-in-rust/>, goes into
>detail about why `Pin` and `Unpin` are necessary. Also don't forget to consult
>the standard library documentation:
>- `pin` _module_: <https://doc.rust-lang.org/std/pin/index.html>
>- `Pin` _struct_: <https://doc.rust-lang.org/std/pin/struct.Pin.html>
>- `Unpin` marker _trait_: <https://doc.rust-lang.org/std/marker/trait.Unpin.html>

Apart from our `Stream` argument, we also need a upper bound on the number of
bytes to be written. A `Stream` can be infinite, but unfortunaly, computer
storage is not.

Using the above considerations, let us outline our function:

```rust

    // ... inside Storage trait

    async fn append<XBuf, XE, X>(
        &mut self,
        buf_stream: &mut X,
        append_threshold: Option<Self::Size>,
    ) -> Result<(Self::Position, Self::Size), Self::Error>
    where
        XBuf: Deref<Target = [u8]>,
        X: Stream<Item = Result<XBuf, XE>> + Unpin,
    {
        /// ...
    }

```

When `append_threshold` is `None`, we attempt to exhaustively read the entire
stream to write to our storage. If it's `Some(thresh)`, we only write upto
`thresh` bytes.

Let's proceed with our implementation:

```rust

    // ... inside Storage trait

    async fn append<XBuf, XE, X>(
        &mut self,
        buf_stream: &mut X,
        append_threshold: Option<Self::Size>,
    ) -> Result<(Self::Position, Self::Size), Self::Error>
    where
        XBuf: Deref<Target = [u8]>,
        X: Stream<Item = Result<XBuf, XE>> + Unpin,
    {
        let (mut bytes_written, pos) = (num::zero(), self.size());

        while let Some(buf) = buf_stream.next().await {
             let stream_read_result = match (buf, append_threshold) {
                (Ok(buf), Some(w_cap)) => {
                    match Self::Size::from_usize(buf.deref().len()) {
                        Some(buf_len) if buf_len + bytes_written <= w_cap => Ok(buf),
                        _ => Err::<XBuf, Self::Error>(StreamUnexpectedLength.into()),
                    }
                }
                (Ok(buf), None) => Ok(buf),
                (Err(_), _) => Err(StreamUnexpectedLength.into()),
            };

            // ...
        }
    }

```

We maintain a counter for the number of bytes already written: `bytes_written`.
For every byte slice read from the stream, we check if it can be accomodated in
our storage in accordance with the `append_threshold`. If not, we error out.

We also keep the write position around in `pos`. It is simply the size of this
storage before we append anything.

Now, let's try to append it:

```rust

        // ... inside Storage::append

        while let Some(buf) = buf_stream.next().await {
             let stream_read_result = match (buf, append_threshold) {
                (Ok(buf), Some(w_cap)) => {
                    match Self::Size::from_usize(buf.deref().len()) {
                        Some(buf_len) if buf_len + bytes_written <= w_cap => Ok(buf),
                        _ => Err::<XBuf, Self::Error>(StreamUnexpectedLength.into()),
                    }
                }
                (Ok(buf), None) => Ok(buf),
                (Err(_), _) => Err(StreamUnexpectedLength.into()),
            };

            let append_result = match stream_read_result {
                Ok(buf) => self.append_slice(buf.deref()).await,
                Err(_) => Err(StreamUnexpectedLength.into()),
            };

            // ...
        }

```

That's reasonable. We append if possible, and propagate the error. Continuing...

```rust 


        // ... inside Storage::append

        while let Some(buf) = buf_stream.next().await {
             let stream_read_result = match (buf, append_threshold) {
                (Ok(buf), Some(w_cap)) => {
                    match Self::Size::from_usize(buf.deref().len()) {
                        Some(buf_len) if buf_len + bytes_written <= w_cap => Ok(buf),
                        _ => Err::<XBuf, Self::Error>(StreamUnexpectedLength.into()),
                    }
                }
                (Ok(buf), None) => Ok(buf),
                (Err(_), _) => Err(StreamUnexpectedLength.into()),
            };

            let append_result = match stream_read_result {
                Ok(buf) => self.append_slice(buf.deref()).await,
                Err(_) => Err(StreamUnexpectedLength.into()),
            };

            match append_result {
                Ok((_, buf_bytes_w)) => {
                    bytes_written = bytes_written + buf_bytes_w;
                }
                Err(error) => {
                    self.truncate(&pos).await?;
                    return Err(error);
                }
            };
        }

```

So for every byte slice, we add the number of bytes in it to `bytes_written` if
everything goes well. However, if anything goes wrong:
- We rollback all writes by truncating at the position before all writes,
  stored in `pos`.
- We return the error encountered.

Finally, once we exit the loop, we return the position at which the record
stream was written, along with the total bytes written:

```rust

        } // ... end of: while let Some(buf) = buf_stream.next().await {...}

        Ok((pos, bytes_written))

    } // ... end of Storage::append
```

We can coalesce the match blocks together by inilining all the results. Putting
everything all together:

```rust
/// Error to represent undexpect stream termination or overflow, i.e a stream 
/// of unexpected length.
#[derive(Debug)]
pub struct StreamUnexpectedLength;

impl std::fmt::Display for StreamUnexpectedLength {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for StreamUnexpectedLength {}

/// Trait representing a read-append-truncate storage media.
#[async_trait(?Send)]
pub trait Storage:
    AsyncTruncate<Mark = Self::Position, TruncError = Self::Error>
    + AsyncConsume<ConsumeError = Self::Error>
    + Sizable<Size = Self::Position>
{
    /// Type to represent the content bytes of this storage media.
    type Content: Deref<Target = [u8]> + Unpin;

    /// Type to represent data positions inside this storage media.
    type Position: Unsigned + FromPrimitive + ToPrimitive + Sum + Ord + Copy;

    /// Error that can occur during storage operations.
    type Error: std::error::Error + From<StreamUnexpectedLength>;

    /// Appends the given slice of bytes to the end of this storage.
    ///
    /// Implementations must update internal cursor or write pointers, if any,
    /// when implementing this method.
    async fn append_slice(
        &mut self,
        slice: &[u8],
    ) -> Result<(Self::Position, Self::Size), Self::Error>;

    /// Appends a stream of byte slices to the end of this storage.
    ///
    /// This method writes at max `append_threshold` number of bytes from the
    /// given stream of bytes slices. If the provided `append_threshold` is
    /// `None`, no such check is enforced; we attempt to write the entire
    /// stream until it's exhausted.
    ///
    /// The following error scenarios may occur during writing:
    /// - `append_threshold` is `Some(_)`, and the stream contains more bytes
    /// than the threshold
    /// - The stream unexpectedly yields an error when attempting to read the
    /// next byte slice from the stream
    /// - There is a storage error when attempting to write one of the byte
    /// slices from the stream.
    ///
    /// In all of the above error cases, we truncate this storage media with
    /// the size of the storage media before we started the append operation,
    /// effectively rolling back any writes.
    ///
    /// Returns the position where the bytes were written and the number of
    /// bytes written.
    async fn append<XBuf, XE, X>(
        &mut self,
        buf_stream: &mut X,
        append_threshold: Option<Self::Size>,
    ) -> Result<(Self::Position, Self::Size), Self::Error>
    where
        XBuf: Deref<Target = [u8]>,
        X: Stream<Item = Result<XBuf, XE>> + Unpin,
    {
        let (mut bytes_written, pos) = (num::zero(), self.size());

        while let Some(buf) = buf_stream.next().await {
            match match match (buf, append_threshold) {
                (Ok(buf), Some(w_cap)) => {
                    match Self::Size::from_usize(buf.deref().len()) {
                        Some(buf_len) if buf_len + bytes_written <= w_cap => Ok(buf),
                        _ => Err::<XBuf, Self::Error>(StreamUnexpectedLength.into()),
                    }
                }
                (Ok(buf), None) => Ok(buf),
                (Err(_), _) => Err(StreamUnexpectedLength.into()),
            } {
                Ok(buf) => self.append_slice(buf.deref()).await,
                Err(_) => Err(StreamUnexpectedLength.into()),
            } {
                Ok((_, buf_bytes_w)) => {
                    bytes_written = bytes_written + buf_bytes_w;
                }
                Err(error) => {
                    self.truncate(&pos).await?;
                    return Err(error);
                }
            };
        }

        Ok((pos, bytes_written))
    }

    /// Reads `size` number of bytes from the given `position`.
    ///
    /// Returns the bytes read.
    async fn read(
        &self,
        position: &Self::Position,
        size: &Self::Size,
    ) -> Result<Self::Content, Self::Error>;
}
```

Now to answer one of the initial questions, we needed `StreamUnexpectedLength`
as a sentinel error type to represent the error case when the stream
unexpectedly errors out while reading, or has more bytes in total than our
`append_threshold`.

#### `Record` (_struct_)
Before we move on to the concept of a `CommitLog`, we need to abstract a much
more fundamental aspect of our implementation. How do we represent the actual
"records"?

Let's see... a record in the most general sense needs only two things:
- The actual _value_ to be contained in the record
- Some _metadata_ about the record

So, we express that directly:

```rust
pub struct Record<M, T> {
    pub metadata: M,
    pub value: T,
}
```

#### `CommitLog` (_trait_)

Now, with the abstractions presented above, we are ready to express the notion
of a `CommitLog`. The properties of a `CommitLog` are:
1.  It allows reading records from random indices
2.  Naturally it has some index bounds
3.  It allows appending records which may contain a stream of byte slices as it
  value.
4.  It can be truncated at a specific index.
5.  It supports `close()` and `remove()` operations to safely persist or remove
  data respectively.

All of these properties have already been represented with the traits above. We
now use them to define the concept of a `CommitLog`:

```rust
#[async_trait::async_trait(?Send)]
pub trait CommitLog<M, T>:
    AsyncIndexedRead<Value = Record<M, T>, ReadError = Self::Error> // (1, 2)
    + AsyncTruncate<Mark = Self::Idx, TruncError = Self::Error> // (4)
    + AsyncConsume<ConsumeError = Self::Error> // (5)
    + Sizable // Of course, we need to know the total storage size
{
    /// Associated error type for fallible operations 
    type Error: std::error::Error; 

    // (3)
    async fn append<X, XBuf, XE>(&mut self, record: Record<M, X>) -> Result<Self::Idx, Self::Error>
    where
        X: futures_lite::Stream<Item = Result<XBuf, XE>>,
        X: Unpin + 'async_trait,
        XBuf: std::ops::Deref<Target = [u8]>;

    /// Removes expired records i.e records older than the given _expiry_duration.
    ///
    /// The default implementation doesn't remove any records.
    ///
    /// Returns the number of records removed.
    async fn remove_expired(
        &mut self,
        _expiry_duration: std::time::Duration,
    ) -> Result<Self::Idx, Self::Error> {
        // remove zero records by default
        async { Ok(<Self::Idx as num::Zero>::zero()) }.await
    } // ... what's this?
}
```

Optionally, a `CommitLog` implementation might need to remove some records that
are older by a certain measure of time. Let's call them _expired_ records. So
we provide a function for that in case different implementations need it.

---

Using the `Record` struct and the different traits that we have described so
far, we can implement any component of the `SegmentedLog`. These abstractions
form the _basis_ of our implementation.

---

#### `Index` (_struct_)
Let's start with our first direct component of our indexed segmented log, the
`Index`.

First, we need to answer two primary questions:
- What kind of data are we storing?
- In what layout will we store the said data?

So recall, at a high level, an `Index` is logical mapping from indices to byte
positions on storage.

So this at least has store 2-tuples of the form: `(record_index, record_position)`

Now we need two additional data points:
- Number of bytes in the record i.e `record_length`
- A checksum of the contents of the record, e.g. crc32

These two datapoints are essential to verify if the record data is valid or
corrupted on the storage media. ("storage media" = `Storage` trait impl.)

So we arrive at this 4-tuple: `(checksum, length, index, position)`

Let's call this an `IndexRecord`.

Now, an `Index` stores index records sequentially. So:
```
index_record[i+1].index = index_record[i].index + 1
```

Now, all these 4-tuples need the exact same number of bytes to be stored. Let's
call this size `IRSZ`. If the index records are laid out sequentially:, every
index record will be at a position which is an integral multiple of `IRSZ`:

```
## storage (Index)

(checksum, length, index, position) @ storage::position = 0
(checksum, length, index, position) @ storage::position = 1 x IRSZ
(checksum, length, index, position) @ storage::position = 2 x IRSZ
(checksum, length, index, position) @ storage::position = 3 x IRSZ
...
```

>Note: the `position` in the tuple refers to the position of the actual `Record`
>in `Store`. `storage::position` here refers to the position within the `Index`
>file (`Storage` impl).

Due to this property, the index can be derived from the position of the record
itself. The number of records is simply: 

```
len(Index) = size(Index) / IRSZ
```

Using this, we can conclude that storing the `index` in each `IndexRecord` is
redundant.

However, an `Index` can start from an arbitrary high index. Let's call this the
`base_index`. So if we store a marker record of sorts, the contains the
`base_index`, and then store all the index records after it sequentially, we
can say:

```
// simple row major address calculation
index_record_position_i = size(base_marker) + i * IRSZ
```

So now we can lay out our `IndexRecord` instances on storage as follows:

```
## storage (Index)

[base_index_marker]          @ storage::position = 0
(checksum, length, position) @ storage::position = size(base_index_marker) + 0
(checksum, length, position) @ storage::position = size(base_index_marker) + 1 x IRSZ
(checksum, length, position) @ storage::position = size(base_index_marker) + 2 x IRSZ
(checksum, length, position) @ storage::position = size(base_index_marker) + 3 x IRSZ
...
```

Now, number of records is calculated as:
```
// number of records in Index
len(Index) = (size(Index) - size(base_index_marker)) / IRSZ
```

Now let's finalize the bytewise layout on storage:

```
## IndexBaseMarker (size = 16 bytes)
┌───────────────────────────┬──────────────────────────┐
│ base_index: u64 ([u8; 8]) │ _padding:  u64 ([u8; 8]) │
└───────────────────────────┴──────────────────────────┘

## IndexRecord (size = 16 bytes)
┌─────────────────────────┬───────────────────────┬─────────────────────────┐
│ checksum: u64 ([u8; 8]) │ length: u32 ([u8; 4]) │ position: u32 ([u8; 4]) │
└─────────────────────────┴───────────────────────┴─────────────────────────┘
```

We add padding to the `IndexBaseMarker` to keep it aligned with `IndexRecord`.

We represent these records as follows:

```rust
pub struct IndexBaseMarker {
    pub base_index: u64,
    _padding: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IndexRecord {
    pub checksum: u64,
    pub length: u32,
    pub position: u32,
}
```

Since we 32 bytes to represent positions of records, a `Segment` can contain
only be upto 4GiB (2<sup>32</sup> unique byte positions = 2<sup>32</sup> bytes =
4GiB). Practically speaking, segments generally don't exceed 1GB. If segments
are too big, individual segments are difficult to move around. So this limit is
not a problem.

We use binary encoding to store these records.

Now we could use [_serde_](https://serde.rs/) and
[_bincode_](https://docs.rs/bincode/latest/bincode/) to serialize these records
on `Storage` _impls_. However, since these records will be serialized and
deserialized fairly often, I wanted to serialize in constant space, with a
simple API.

First, let us generalize over both `IndexBaseMarker` and `IndexRecord`. We need
to formalize an entity with the folowing properties:
- It has a known size at compile time
- It can be read from and written to any storage

We can express this directly:

```rust
trait SizedRecord: Sized {
    fn write<W: Write>(&self, dest: &mut W) -> std::io::Result<()>;

    fn read<R: Read>(source: &mut R) -> std::io::Result<Self>;
}
```

Now we need a kind of `SizedRecord` that can be stored on a `Storage` _impl_.
Let's call it `PersistentSizedRecord`:

```rust
/// Wrapper struct to enable `SizedRecord` impls to be stored on Storage impls.
///
/// REPR_SIZE is the number of bytes required to store the inner SizedRecord.
struct PersistentSizedRecord<SR, const REPR_SIZE: usize>(SR);

impl<SR: SizedRecord, const REPR_SIZE: usize> PersistentSizedRecord<SR, REPR_SIZE> {
    async fn read_at<S>(source: &S, position: &S::Position) -> Result<Self, IndexError<S::Error>>
    where
        S: Storage,
    {
        // read exactly REPR_SIZE bytes from source Storage impl. at the given position
        let record_bytes = source
            .read(
                position,
                &<S::Size as FromPrimitive>::from_usize(REPR_SIZE)
                    .ok_or(IndexError::IncompatibleSizeType)?,
            )
            .await
            .map_err(IndexError::StorageError)?; // read bytes for record

        let mut cursor = Cursor::new(record_bytes.deref()); // wrap to a Read impl.

        SR::read(&mut cursor).map(Self).map_err(IndexError::IoError) // deserialize
    }

    async fn append_to<S>(&self, dest: &mut S) -> Result<S::Position, IndexError<S::Error>>
    where
        S: Storage,
    {
        let mut buffer = [0_u8; REPR_SIZE]; // buffer to store Serialized record
        let mut cursor = Cursor::new(&mut buffer as &mut [u8]); // wrap to a Write impl.

        self.0.write(&mut cursor).map_err(IndexError::IoError)?; // serialize

        let (position, _) = dest
            .append_slice(&buffer)
            .await
            .map_err(IndexError::StorageError)?; // append to storage

        Ok(position)
    }
}
```

Now we simply need to implement `SizedRecord` for `IndexBaseMarker` and
`IndexRecord`:

```rust
impl SizedRecord for IndexBaseMarker {
    fn write<W: Write>(&self, dest: &mut W) -> std::io::Result<()> {
        dest.write_u64::<LittleEndian>(self.base_index)?;

        Ok(())
    }

    fn read<R: Read>(source: &mut R) -> std::io::Result<Self> {
        let base_index = source.read_u64::<LittleEndian>()?;

        Ok(Self {
            base_index,
            _padding: 0_u64,
        })
    }
}

impl SizedRecord for IndexRecord {
    fn write<W: Write>(&self, dest: &mut W) -> std::io::Result<()> {
        dest.write_u64::<LittleEndian>(self.checksum)?;
        dest.write_u32::<LittleEndian>(self.length)?;
        dest.write_u32::<LittleEndian>(self.position)?;

        Ok(())
    }

    fn read<R: Read>(source: &mut R) -> std::io::Result<Self> {
        let checksum = source.read_u64::<LittleEndian>()?;
        let length = source.read_u32::<LittleEndian>()?;
        let position = source.read_u32::<LittleEndian>()?;

        Ok(IndexRecord {
            checksum,
            length,
            position,
        })
    }
}
```

>[ Quiz 💡]: We dont read or write the `_padding` bytes in our `IndexBaseMarker`
>`SizedRecord` _impl_. So how is it still aligned?
>
>[ A ]: Remember that we pass in a _const_ generic parameter `REPR_SIZE` when
>creating a `PersistentSizedRecord`. When writing or reading, we always read
>`REPR_SIZE` number of bytes, regardless of how we serialize or deserialize our
>`IndexRecord` or `IndexBaseMarker`. In this case we just pass a `const usize`
>with value `16`.

We also declare some useful constants to keep things consistent:

```rust
/// Extension used by backing files for Index instances.
pub const INDEX_FILE_EXTENSION: &str = "index";

/// Number of bytes required for storing the base marker.
pub const INDEX_BASE_MARKER_LENGTH: usize = 16;

/// Number of bytes required for storing the record header.
pub const INDEX_RECORD_LENGTH: usize = 16;

/// Lowest underlying storage position
pub const INDEX_BASE_POSITION: u64 = 0;
```

Before we proceed with our `Index` implementation, let us do a quick back of
the handle estimate on how big `Index` files can be.

Every `IndexRecord` is 16 bytes.
So for every `Record` we have 16 bytes.
Let's assume that `Record` sizes are `1KB` on average.
Let's assume that `Segment` files are `1GB` on average.

So we can calculate as follows:
```
        1GB segment file = pow(10, 6) KB = pow(10, 6) records

         1 * 1KB record  = 1 * 16B IndexRecord
pow(10, 6) * 1KB records = pow(10, 6) * 16B IndexRecord
                         = 16MB

Therefore,
        1GB segment file => 16MB Index overhead

e.g. 10 * 1GB segment files => 10 * 16MB Index files = 160 MB overhead

     ITB total data through 1000 segmented files => 16GB overhead   
```

Keep this calculation in mind as we proceed through our implementation.
...

## Closing notes

This concludes the implementation.

## References

We utilized the following resources as references for this blog post:

{% references() %}

Lamport, Leslie. "Time, clocks, and the ordering of events in a distributed
system." *Concurrency: the Works of Leslie Lamport.* 2019. 179-196.
[https://dl.acm.org/doi/pdf/10.1145/359545.359563](https://dl.acm.org/doi/pdf/10.1145/359545.359563)

Jay Kreps. "The Log: What every software engineer should know about real-time
data's unifying abstraction." *LinkedIn engineering blog.* 2013.
<https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying>

Kreps, Jay, Neha Narkhede, and Jun Rao. "Kafka: A distributed messaging system
for log processing." _Proceedings of the NetDB._ Vol. 11. No. 2011. 2011.
<https://pages.cs.wisc.edu/~akella/CS744/F17/838-CloudPapers/Kafka.pdf>

{% end %}

[^1]: A lamport clock is a logical counter to establish causality between two
events. Since it's decoupled from wall-clock time, it's used in
distributed-systems for ordering events.
