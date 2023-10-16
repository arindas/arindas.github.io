+++
title = "Building Segmented Logs in Rust: From Theory to Production!"
date = 2023-10-10
description = "Explore a Rust implementation of the persistence mechanism behind message-queues and write-ahead-logs in databases. Embark on a journey from the theoretical underpinnings to a production grade implementation of the segmented-log data structure."
draft = true

[taxonomies]
tags = ["rust", "tokio", "segmented-log", "message-queue", "distributed-systems"]

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

While I would love to discuss _testing_, _benchmarking_ and _profiling_, this
blog post is becoming quite lengthy. So, please look them up on the repository
provided above.

>Note: Some of the identifier names might be different on the repository. I
>have refactored the code sections here to improve readability on various
>devices. Also there are more comments here to make it easier to understand.

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

##### A sample `Storage` _impl_.

Let's explore a `tokio::fs::File` based implementation of `Storage`:

First, let's outline our _struct_:

```rust
pub struct StdSeekReadFileStorage {
    storage: RwLock<BufWriter<TokioFile>>,
    backing_file_path: PathBuf,

    size: u64,
}
```

>`TokioFile` is an alias for `tokio::fs::File`. It's defined in a use directive
>in the source.

We need a buffered writer over our file to avoid hitting the file too many
times for small writes. Since our workload will largely be composed of small
writes and reads, this is important.

The need for `RwLock` will be clear in a moment.

Next, let's proceed with the constructor for this struct:

```rust
impl StdSeekReadFileStorage {
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self, StdSeekReadFileStorageError> {
        let backing_file_path = path.as_ref().to_path_buf();

        let storage = Self::obtain_backing_storage(&backing_file_path).await?;

        let initial_size = storage
            .metadata()
            .await
            .map_err(StdSeekReadFileStorageError::IoError)?
            .len();

        Ok(Self {
            storage: RwLock::new(BufWriter::new(storage)),
            backing_file_path,
            size: initial_size,
        })
    }

    async fn obtain_backing_storage<P: AsRef<Path>>(
        path: P,
    ) -> Result<TokioFile, StdSeekReadFileStorageError> {
        OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .read(true)
            .open(path)
            .await
            .map_err(StdSeekReadFileStorageError::IoError)
    }
}
```

As you can see, for the underlying storage file, we enable the following flags:
- `write`: enables writing to the file
- `append`: new writes are appended (as opposed to truncating the file before
  writes)
- `create`: if the file doesn't exist, it's created
- `read`: enable reading from the file

Now before we implement the `Storage` _trait_ for `StdSeekReadFileStorage`, we
need to implement `Storage`'s inherited traits. Let's proceed one by one.

First, we have `Sizable`:

```rust
impl Sizable for StdSeekReadFileStorage {
    type Size = u64;

    fn size(&self) -> Self::Size {
        self.size
    }
}
```

Next, we implement `AsyncTruncate`:

```rust
#[async_trait(?Send)]
impl AsyncTruncate for StdSeekReadFileStorage {
    type Mark = u64;

    type TruncError = StdSeekReadFileStorageError;

    async fn truncate(&mut self, position: &Self::Mark) -> Result<(), Self::TruncError> {
        // before truncating, flush all writes
        self.storage
            .write()
            .await
            .flush()
            .await
            .map_err(StdSeekReadFileStorageError::IoError)?;

        // reopen file directly for truncation
        let writer = Self::obtain_backing_storage(&self.backing_file_path).await?;

        // truncate at the given position
        writer
            .set_len(*position)
            .await
            .map_err(StdSeekReadFileStorageError::IoError)?;

        // close old file handle and assign thr new file handle to storage
        self.storage = RwLock::new(BufWriter::new(writer));
        self.size = *position; // update size after truncation

        Ok(())
    }
}
```

We also need `AsyncConsume`:

```rust
#[async_trait(?Send)]
impl AsyncConsume for StdSeekReadFileStorage {
    type ConsumeError = StdSeekReadFileStorageError;

    async fn remove(mut self) -> Result<(), Self::ConsumeError> {
        let backing_file_path = self.backing_file_path.clone();

        self.close().await?;

        tokio::fs::remove_file(&backing_file_path)
            .await
            .map_err(StdSeekReadFileStorageError::IoError)
    }

    async fn close(mut self) -> Result<(), Self::ConsumeError> {
        self.storage
            .write()
            .await
            .flush()
            .await
            .map_err(StdSeekReadFileStorageError::IoError)
    }
}
```

With the pre-requisites ready, let's proceed with our `Storage` _impl_:

```rust
#[async_trait(?Send)]
impl Storage for StdSeekReadFileStorage {
    type Content = Vec<u8>;

    type Position = u64;

    type Error = StdSeekReadFileStorageError;

    async fn append_slice(
        &mut self,
        slice: &[u8],
    ) -> Result<(Self::Position, Self::Size), Self::Error> {
        let current_position = self.size;

        // write to storage using he BufWriter
        self.storage
            .write()
            .await
            .write_all(slice)
            .await
            .map_err(StdSeekReadFileStorageError::IoError)?;

        let bytes_written = slice.len() as u64;

        self.size += bytes_written; // update storage size

        Ok((current_position, bytes_written))
    }

    async fn read(
        &self,
        position: &Self::Position,
        size: &Self::Size,
    ) -> Result<Self::Content, Self::Error> {
        // validate that the record to be read is within the written area
        if *position + *size > self.size {
            return Err(StdSeekReadFileStorageError::ReadBeyondWrittenArea);
        }

        // pre-allocate buffer to use for reading
        let mut read_buf = vec![0_u8; *size as usize];

        // acquire &mut storage from behind &self using RwLock::write
        let mut storage = self.storage.write().await;

        // seek to read position
        storage
            .seek(io::SeekFrom::Start(*position))
            .await
            .map_err(StdSeekReadFileStorageError::IoError)?;

        // read required number of bytes
        storage
            .read_exact(&mut read_buf)
            .await
            .map_err(StdSeekReadFileStorageError::IoError)?;

        // seek to end of file using current size of file
        storage
            .seek(io::SeekFrom::Start(self.size))
            .await
            .map_err(StdSeekReadFileStorageError::IoError)?;

        Ok(read_buf)
    }
}
```

Notice that the `Storage` trait uses a `&self` for `Storage::read`. This was to
support idempotent operations.

In our case, we need to update the seek position of the file, from behind a
`&self`. So we need some interior mutability to achieve this. Hence the `RwLock`.

Hopefully, the need for the `RwLock` is clear now. In retrospect, we could also
have used a `Mutex` but using a `RwLock` keeps the option of multiple readers
for read only operations open.

Note that, the read operation is still idempotent as we restore the old file
position after reading.

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

impl<SR, const REPR_SIZE: usize> PersistentSizedRecord<SR, REPR_SIZE> {
    fn into_inner(self) -> SR {
        self.0
    }
}

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

Next, we implement the `SizedRecord` _trait_ for `IndexBaseMarker` and
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
            1GB Segment == pow(10, 6) * 1KB Record

         1 * 1KB Record =>          1 * 16B IndexRecord

pow(10, 6) * 1KB Record => pow(10, 6) * 16B IndexRecord
                        => 16MB Index

Or,         1GB Segment => 16MB Index (Result)


e.g. 10 * 1GB segment files => 10 * 16MB Index files = 160 MB overhead

     ITB total data through 1000 segment files => 16GB overhead
```

Keep this calculation in mind as we proceed through our implementation.

With the groundwork ready, let's begin our `Index` implementation:

```rust
pub struct Index<S, Idx> {
    /// In memory cache of IndexRecord instaqnces
    index_records: Option<Vec<IndexRecord>>,

    base_index: Idx, /// Index stored in the first IndexRecord
    next_index: Idx, /// Index to be stored in the next IndexRecord to be added

    storage: S,      /// Underlying storage for Index
}
```

Why do we need an in-memory cache in the fist place? Well `IndexRecord`
instances are fairly small and there are usually few of them (`<= 1000`) in an
`Index`. A simple in-memory cache makes sense rather than hitting the storage
everytime. (We could probably `mmap()` but this is simple enough.)

Alright then, why make it optional?

Recall that for `1TB` of data with `1KB` record size, we end up having `16GB`
of `Index` overhead. It's clearly not practical allocating this amount of
memory, since we expect our system to able to handle this scale.

So we make caching `IndexRecord` instances optional. This would enable us
to decide which `Index` instances to cache based on access patterns.

For instance, we could maintain an `LRUCache` of `Index` instances that are
currently cached. When an `Index` outside of the `LRUCache` is accessed, we add
it to the `LRUCache`. When an `Index` from within the `LRUCache` is accessed,
we update the `LRUCache` accordingly.  The `LRUCache` will have some maximum
capacity, which decides the maximum number of `Index` instances that can be
cached at the same time. We could replace `LRUCache` with other kinds of cache
(e.g. `LFUCache`) for different performance characteristics. The `Index` files
are still persisted on storage so there is no loss of data.

>I wanted this implementation to handle `1TB` of data on a [Raspberry Pi
>3B](https://www.raspberrypi.com/products/raspberry-pi-3-model-b/).
>Unfortunately, it has only `1GB` RAM. However, if we enforce a limit that only
>`10` `Index` instances are cached at a time (e.g. by setting the `LRUCache`
>max capacity to `10`), that would be a `160MB` overhead. That would make this
>implementation usable on an RPi 3B, albeit at the cost of some latency.
>
>For storage, I can connect a 1TB external hard disk to the RPi 3B and proceed
>as usual.

Now, let's define some utilities for constructing `Index` instances.

```rust
impl<S, Idx> Index<S, Idx>
where
    S: Storage,
    Idx: Unsigned + FromPrimitive + Copy + Eq,
{

    /// Estimates the numer of IndexRecord instances in the given Storage impl ref.
    ///
    /// Returns the number of IndexRecord instances estimated to be present.
    pub fn estimated_index_records_len_in_storage(
        storage: &S,
    ) -> Result<usize, IndexError<S::Error>> {
        let index_storage_size = storage // size of the given storage for Index
            .size()
            .to_usize()
            .ok_or(IndexError::IncompatibleSizeType)?;

        // len(Index) = (size(Index) - size(IndexBaseMarker)) / size(IndexRecord)
        let estimated_index_records_len =
            index_storage_size.saturating_sub(INDEX_BASE_MARKER_LENGTH) / INDEX_RECORD_LENGTH;

        Ok(estimated_index_records_len)
    }

    /// Obtains the base_index by reading the IndexBaseMarker from the given storage.
    ///
    /// Returns the base index.
    pub async fn base_index_from_storage(storage: &S) -> Result<Idx, IndexError<S::Error>> {
        // read the index base marker from the base position (0)
        let index_base_marker =
            PersistentSizedRecord::<IndexBaseMarker, INDEX_BASE_MARKER_LENGTH>::read_at(
                storage,
                &u64_as_position!(INDEX_BASE_POSITION, S::Position)?,
            )
            .await
            .map(|x| x.into_inner());

        // map Result<IndexBaseMarker, ...> to Result<Idx, ...>
        index_base_marker
            .map(|x| x.base_index)
            .and_then(|x| u64_as_idx!(x, Idx))
    }

    /// Reads the IndexRecord instances present in the given Storage impl ref.
    ///
    /// Returns a Vec of IndexRecord instances.
    pub async fn index_records_from_storage(
        storage: &S,
    ) -> Result<Vec<IndexRecord>, IndexError<S::Error>> {
        // start reading after the IndexBaseMarker
        let mut position = INDEX_BASE_MARKER_LENGTH as u64;

        let estimated_index_records_len = Self::estimated_index_records_len_in_storage(storage)?;

        // preallocate the vector for storing IndexRecord instances
        let mut index_records = Vec::<IndexRecord>::with_capacity(estimated_index_records_len);

        // while index records can be read without error
        while let Ok(index_record) =
            PersistentSizedRecord::<IndexRecord, INDEX_RECORD_LENGTH>::read_at(
                storage,
                &u64_as_position!(position, S::Position)?,
            )
            .await
        {
            // append the IndexRecord read just now to the Vec<IndexRecord>
            index_records.push(index_record.into_inner());

            // advance to the next position for reading an IndexRecord
            position += INDEX_RECORD_LENGTH as u64;
        }

        index_records.shrink_to_fit(); // release empty space left, if any

        // cross verify the number of index records read
        if index_records.len() != estimated_index_records_len {
            Err(IndexError::InconsistentIndexSize)
        } else {
            Ok(index_records)
        }
    }

    /// Cross validates the given base index against the one in the storage.
    ///
    /// Returns the validated base index.
    pub async fn validated_base_index(
        storage: &S,
        base_index: Option<Idx>,
    ) -> Result<Idx, IndexError<S::Error>> {
        // read the base index from the given storage
        let read_base_index = Self::base_index_from_storage(storage).await.ok();

        match (read_base_index, base_index) {
            // a. error out if neither in storage, nor provided
            (None, None) => Err(IndexError::NoBaseIndexFound),

            // b. either only in storage or only provided, choose what is present
            (None, Some(base_index)) => Ok(base_index),
            (Some(base_index), None) => Ok(base_index),

            // c. present in both storage, as well as provided

            // c.1. conflicting, error out
            (Some(read), Some(provided)) if read != provided => Err(IndexError::BaseIndexMismatch),

            // c.2. no conflict, choose provided (no difference)
            (Some(_), Some(provided)) => Ok(provided),
        }
    }

    // ...
}

```

Next, we define our constructors for `Index`:

```rust
impl<S, Idx> Index<S, Idx>
where
    S: Storage,
    Idx: Unsigned + FromPrimitive + Copy + Eq,
{
    // ...

    /// Creates an Index instance from a `Storage` impl instance and an optional base index.
    ///
    /// Reads the IndexRecord instances present in the given storage and caches them.
    ///
    /// Returns an Index instance.
    pub async fn with_storage_and_base_index_option(
        storage: S,
        base_index: Option<Idx>,
    ) -> Result<Self, IndexError<S::Error>> {
        // cross validates the given base index with the one present on storage.
        let base_index = Self::validated_base_index(&storage, base_index).await?;

        // reads the IndexRecord instances preent in the provided storage
        let index_records = Self::index_records_from_storage(&storage).await?;

        let len = index_records.len() as u64;

        let next_index = base_index + u64_as_idx!(len, Idx)?;

        Ok(Self {
            index_records: Some(index_records),
            base_index,
            next_index,
            storage,
        })
    }

    pub async fn with_storage_and_base_index(
        storage: S,
        base_index: Idx,
    ) -> Result<Self, IndexError<S::Error>> {
        Self::with_storage_and_base_index_option(storage, Some(base_index)).await
    }

    pub async fn with_storage(storage: S) -> Result<Self, IndexError<S::Error>> {
        Self::with_storage_and_base_index_option(storage, None).await
    }


    /// Creates an Index with the given storage, cached index records and a
    /// validated base index.
    ///
    /// This function doesn't touch the provided Storage impl instance,
    /// other than reading its size. The cached index record instances
    /// are used as is. If no cache if provided, then the created Index
    /// is not cached.
    ///
    /// This function is primarily useful when flushing an Index instance by
    /// closing the underlying storage and re-opening it, without reading all
    /// the IndexRecord instances again.
    pub fn with_storage_index_records_option_and_validated_base_index(
        storage: S,
        index_records: Option<Vec<IndexRecord>>,
        validated_base_index: Idx,
    ) -> Result<Self, IndexError<S::Error>> {
        let len = Self::estimated_index_records_len_in_storage(&storage)? as u64;
        let next_index = validated_base_index + u64_as_idx!(len, Idx)?;

        Ok(Self {
            index_records,
            base_index: validated_base_index,
            next_index,
            storage,
        })
    }

    // ...
}
```

Next, we define some functions for managing caching behaviour:

```rust
impl<S, Idx> Index<S, Idx>
where
    S: Storage,
    Idx: Unsigned + FromPrimitive + Copy + Eq,
{
    // ...

    /// Takes the cached index records from the Index, leaving it uncached.
    pub fn take_cached_index_records(&mut self) -> Option<Vec<IndexRecord>> {
        self.index_records.take()
    }

    pub fn cached_index_records(&self) -> Option<&Vec<IndexRecord>> {
        self.index_records.as_ref()
    }

    /// Caches this Index if not already cached.
    pub async fn cache(&mut self) -> Result<(), IndexError<S::Error>> {
        if self.index_records.as_ref().is_some() {
            return Ok(());
        }

        self.index_records = Some(Self::index_records_from_storage(&self.storage).await?);

        Ok(())
    }
}

```

Some minor utilities for ease of implementation:

```rust
impl<S, Idx> Index<S, Idx>
where
    S: Default,
    Idx: Copy,
{
    pub fn with_base_index(base_index: Idx) -> Self {
        Self {
            index_records: Some(Vec::new()),
            base_index,
            next_index: base_index,
            storage: S::default(),
        }
    }
}

impl<S: Storage, Idx> Sizable for Index<S, Idx> {
    type Size = S::Size;

    fn size(&self) -> Self::Size {
        self.storage.size()
    }
}

impl<S: Storage, Idx> Index<S, Idx> {

    /// Returns the position for the IndexRecord corresponding to the given
    /// normalized_index on the underlying storage.
    #[inline]
    fn index_record_position(normalized_index: usize) -> Result<S::Position, IndexError<S::Error>> {
        let position = (INDEX_BASE_MARKER_LENGTH + INDEX_RECORD_LENGTH * normalized_index) as u64;
        u64_as_position!(position, S::Position)
    }
}

impl<S, Idx> Index<S, Idx>
where
    S: Storage,
    Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy,
{

    /// Maps the given Idx to a usize in [0, len(Index))
    #[inline]
    fn internal_normalized_index(&self, idx: &Idx) -> Result<usize, IndexError<S::Error>> {
        self.normalize_index(idx)
            .ok_or(IndexError::IndexOutOfBounds)?
            .to_usize()
            .ok_or(IndexError::IncompatibleIdxType)
    }
}
```

Now, we move on to the primary responsiblities of our Index.

First, let's implement a mechanism to read `IndexRecord` instances from our `Index`:

```rust
#[async_trait::async_trait(?Send)]
impl<S, Idx> AsyncIndexedRead for Index<S, Idx>
where
    S: Storage,
    Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy,
{
    type ReadError = IndexError<S::Error>;

    type Idx = Idx;

    /// The type of value to be read
    type Value = IndexRecord;

    fn highest_index(&self) -> Self::Idx {
        self.next_index
    }

    fn lowest_index(&self) -> Self::Idx {
        self.base_index
    }

    /// Reads the IndexRecord corresponding to the given idx.
    async fn read(&self, idx: &Self::Idx) -> Result<Self::Value, Self::ReadError> {
        let normalized_index = self.internal_normalized_index(idx)?;

        // If this index is cached, read from the cached Vec of IndexRecord instances
        if let Some(index_records) = self.index_records.as_ref() {
            index_records
                .get(normalized_index)
                .ok_or(IndexError::IndexGapEncountered)
                .map(|&x| x)
        } else { // otherwise, read from the underlying storage
            PersistentSizedRecord::<IndexRecord, INDEX_RECORD_LENGTH>::read_at(
                &self.storage,
                &Self::index_record_position(normalized_index)?,
            )
            .await
            .map(|x| x.into_inner())
        }
    }
}

```

Next, we need a mechanism to append `IndexRecord` instances to our `Index`:

```rust
impl<S, Idx> Index<S, Idx>
where
    S: Storage,
    Idx: Unsigned + ToPrimitive + Copy,
{
    pub async fn append(&mut self, index_record: IndexRecord) -> Result<Idx, IndexError<S::Error>> {
        let write_index = self.next_index;

        // If this Index is empty, we need to write the IndexBaseMarker first before
        // writing any IndexRecord
        if write_index == self.base_index {
            PersistentSizedRecord::<IndexBaseMarker, INDEX_BASE_MARKER_LENGTH>(
                IndexBaseMarker::new(idx_as_u64!(write_index, Idx)?),
            )
            .append_to(&mut self.storage)
            .await?;
        }

        // Append the IndexRecord to storage
        PersistentSizedRecord::<IndexRecord, INDEX_RECORD_LENGTH>(index_record)
            .append_to(&mut self.storage)
            .await?;

        // If this Ineex is cached, also append the IndexRecord to the cache
        if let Some(index_records) = self.index_records.as_mut() {
            index_records.push(index_record);
        }

        self.next_index = write_index + Idx::one(); // update the next_index
        Ok(write_index)
    }
}
```

We need to be able to truncate our `Index`:

```rust
#[async_trait::async_trait(?Send)]
impl<S, Idx> AsyncTruncate for Index<S, Idx>
where
    S: Storage,
    Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy,
{
    type TruncError = IndexError<S::Error>;

    type Mark = Idx;

    /// Truncates this Index at the given Index. IndexRecord instances at
    /// indices >= idx are removed.
    async fn truncate(&mut self, idx: &Self::Mark) -> Result<(), Self::TruncError> {
        let normalized_index = self.internal_normalized_index(idx)?;

        self.storage
            .truncate(&Self::index_record_position(normalized_index)?)
            .await
            .map_err(IndexError::StorageError)?;

        if let Some(index_records) = self.index_records.as_mut() {
            index_records.truncate(normalized_index);
        }

        self.next_index = *idx;

        Ok(())
    }
}
```

Finally, we define how to _close_ or _remove_ our `Index`:

```rust
#[async_trait::async_trait(?Send)]
impl<S: Storage, Idx> AsyncConsume for Index<S, Idx> {
    type ConsumeError = IndexError<S::Error>;

    async fn remove(self) -> Result<(), Self::ConsumeError> {
        self.storage
            .remove()
            .await
            .map_err(IndexError::StorageError)
    }

    async fn close(self) -> Result<(), Self::ConsumeError> {
        self.storage.close().await.map_err(IndexError::StorageError)
    }
}
```

Notice how all of the primary functions of our `Index` are supported by the
_traits_ we wrote earlier.


#### `Store` (_struct_)

Now that we have our `Index` ready, we can get started with our backing
`Store`. `Store` is responsible for persiting the record data to `Storage`.

So remember that we have to validate the record bytes persisted using `Store`
with the `checksum` and `length`? To make it easier to work with it, we create
a virtual `RecordHeader`. This virtual `RecordHeader` is never actually
persisted, but it is computed from the bytes to be written or bytes that are
read from the storage.

```rust
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RecordHeader {
    pub checksum: u64,
    pub length: u64,
}
```

>In a previous index-less segmented-log implementation, `RecordHeader`
>instances used to be persisted right before every record on the `Store`. Once
>we moved record `position`, `checksum` and `length` metadata to the `Index`,
>it was no longer necessary to persist the `RecordHeader`.

We only need a constructor for `RecordHeader`:

```rust
impl RecordHeader {
    /// Computes the RecordHeader for the given serialized record bytes
    ///
    /// Returns a RecordHeader instance
    pub fn compute<H>(record_bytes: &[u8]) -> Self
    where
        H: Hasher + Default,
    {
        // compute the hash of the given record bytes
        let mut hasher = H::default();
        hasher.write(record_bytes);
        let checksum = hasher.finish();

        RecordHeader {
            checksum,
            length: record_bytes.len() as u64,
        }
    }
}
```

Now we can proceed with our `Store` implementation:

```rust
pub struct Store<S, H> {
    storage: S, /// Underlying storage

    /// Stores generic parameter used for Hasher impl.
    _phantom_data: PhantomData<H>,
}

impl<S, H> Store<S, H>
where
    S: Storage,
    H: Hasher + Default,
{
    /// Reads the bytes for the record persisted at the given position with
    /// the provided RecordHeader.
    ///
    /// This method reads and verifies the bytes read with the given
    /// RecordHeader by matching the checksum and length.
    ///
    /// Returns the bytes read for the record.
    pub async fn read(
        &self,
        position: &S::Position,
        record_header: &RecordHeader,
    ) -> Result<S::Content, StoreError<S::Error>> {
        // if this store is empty, error out
        if self.size() == u64_as_size!(0_u64, S::Size)? {
            return Err(StoreError::ReadOnEmptyStore);
        }

        // translate record_length usize to S::Size
        let record_length = record_header.length;
        let record_size = u64_as_size!(record_length, S::Size)?;

        // read the record bytes
        let record_bytes = self
            .storage
            .read(position, &record_size)
            .await
            .map_err(StoreError::StorageError)?;

        // cross verify the checksum and length of the bytes read
        if &RecordHeader::compute::<H>(&record_bytes) != record_header {
            return Err(StoreError::RecordHeaderMismatch);
        }

        Ok(record_bytes)
    }

    /// Appends the serialized stream of bytes slices for a record to this Store.
    ///
    /// Returns the computed RecordHeader for the bytes written.
    pub async fn append<XBuf, X, XE>(
        &mut self,
        stream: X,
        append_threshold: Option<S::Size>,
    ) -> Result<(S::Position, RecordHeader), StoreError<S::Error>>
    where
        XBuf: Deref<Target = [u8]>,
        X: Stream<Item = Result<XBuf, XE>> + Unpin,
    {
        let mut hasher = H::default();

        // compute a running checksum / hash
        let mut stream = stream.map(|x| match x {
            Ok(x) => {
                hasher.write(&x);
                Ok(x)
            }
            Err(e) => Err(e),
        });

        // append the byte slices to storage
        let (position, bytes_written) = self
            .storage
            .append(&mut stream, append_threshold)
            .await
            .map_err(StoreError::StorageError)?;

        // obtain the record header from the computed checksum and
        // length from bytes written
        let record_header = RecordHeader {
            checksum: hasher.finish(),
            length: size_as_u64!(bytes_written, S::Size)?,
        };

        Ok((position, record_header))
    }
}
```

We also need mechanism for constructing `IndexRecord` instances from
`RecordHeader` instances once the record bytes are written to the store;

```rust
impl IndexRecord {
    /// Creates an IndexRecord from a store position and RecordHeader,
    /// presumably from a Store::append.
    pub fn with_position_and_record_header<P: ToPrimitive>(
        position: P,
        record_header: RecordHeader,
    ) -> Option<IndexRecord> {
        Some(IndexRecord {
            checksum: record_header.checksum,
            length: u32::try_from(record_header.length).ok()?,
            position: P::to_u32(&position)?,
        })
    }
}
```

`Store` also has `AsyncTruncate`, `AsyncConsume` and `Sizable` _trait impls_,
where it delegates the implementation to the underlying `Storage` _impl_.

```rust
#[async_trait(?Send)]
impl<S: Storage, H> AsyncTruncate for Store<S, H> {
    type Mark = S::Mark;

    type TruncError = StoreError<S::Error>;

    async fn truncate(&mut self, pos: &Self::Mark) -> Result<(), Self::TruncError> {
        self.storage
            .truncate(pos)
            .await
            .map_err(StoreError::StorageError)
    }
}

#[async_trait(?Send)]
impl<S: Storage, H> AsyncConsume for Store<S, H> {
    type ConsumeError = StoreError<S::Error>;

    async fn remove(self) -> Result<(), Self::ConsumeError> {
        self.storage
            .remove()
            .await
            .map_err(StoreError::StorageError)
    }

    async fn close(self) -> Result<(), Self::ConsumeError> {
        self.storage.close().await.map_err(StoreError::StorageError)
    }
}

impl<S: Storage, H> Sizable for Store<S, H> {
    type Size = S::Size;

    fn size(&self) -> Self::Size {
        self.storage.size()
    }
}
```

Now that we have our `Store` and `Index` ready, we can move on to our `Segment`.

#### `Segment` (_struct_)

As we have discussed before, a `Segment` is the smallest unit in a
`SegmentedLog` that can act as a `CommitLog`.

In our implementation a `Segment` comprises of an `Index` and `Store`. Here's
how it handles _reads_ and _appends_:
- For _reads_, it first looks up the `IndexRecord` in `Index` corresponding to the
  given record index. With the `position`, `length` and `checksum` present in
  the `IndexRecord`, it reads the `Record` serialized bytes from the `Store`.
  It then deserialize the bytes as necessary and returns the `Record` requested.
- For _appends_, it first serializes the given `Record`. Next, it writes the
  serialized bytes to the `Store`. Using the `RecordHeader` and `position`
  obtained from `Store::append`, it creates the `IndexRecord` and appends it to
  the `Index`

Now that we know the needed behaviour, let's proceed with the implementation.

First, we represent the configuration schema for our `Segment`:

```rust
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct Config<Size> {
    pub max_store_size: Size,

    /// Maximum number of bytes by which an append
    /// can exceeed the max_store_size limit
    pub max_store_overflow: Size,

    pub max_index_size: Size,
}
```

When these limits are crossed, a segment is considered "maxed out" and has to
be rotated back as a read segment.

Next, we define our `Segment` _struct_:

```rust
pub struct Segment<S, M, H, Idx, Size, SERP> {
    index: Index<S, Idx>,
    store: Store<S, H>,

    config: Config<Size>,

    created_at: Instant,

    _phantom_date: PhantomData<(M, SERP)>,
}
```

I will clarify the generic parameters in a while. However, we have an
additional requirement:

_We want to enable records stored in the segmented-log, to contain the record
index in the metadata._

In order to achieve this, we create a new struct `MetaWithIdx` and use it as
follows:

```rust
pub mod commit_log {

    // ... original Record struct in commit_log module
    pub type Record<M, T> {
        metadata: M,
        value: T
    }

    // ...

    pub mod segmented_log {

        /// Record metadata with index
        #[derive(Debug, Clone, Copy, Serialize, Deserialize)]
        pub struct MetaWithIdx<M, Idx> {
            pub metadata: M,
            pub index: Option<Idx>, // optional to allow automatic index value on append
        }

        impl<M, Idx> MetaWithIdx<M, Idx>
        where
            Idx: Eq,
        {
            /// Anchores the given MetaWithIdx at the given anchor_idx.
            ///
            /// Returns Some(self) if the contained index is same as
            /// the given anchor_idx, None otherwise.
            pub fn anchored_with_index(self, anchor_idx: Idx) -> Option<Self> {
                let index = match self.index {
                    Some(idx) if idx != anchor_idx => None,
                    _ => Some(anchor_idx),
                }?;

                Some(Self {
                    index: Some(index),
                    ..self
                })
            }
        }

        /// Record with metadata containing the record index
        pub type Record<M, Idx, T> = super::Record<MetaWithIdx<M, Idx>, T>;

        pub mod segment {
            pub struct Segment { /* ... */ }

            // ...
        }

        pub mod index {
            pub struct Index { /* ... */ }

            // ...
        }

        pub mod store {
            pub struct Store { /* ... */ }

            // ...
        }

        // ...
    }

}
```

Here's what I want to highlight:
- We create a _struct_ `MetaWithIdx` to use as the metadata value used for
`commit_log::Record`.
- Next we create a _type alias_ `commit_log::segmented_log::Record` which uses
the `MetaWithIdx` struct for metadata.

Why am I saying all this? Well I did need to clarify the module structure a
bit, but there's another reason.

Let's go back to our `Segment` _struct_ and describe the different generic
parameters:

```rust

pub struct Segment<S, M, H, Idx, Size, SERP> { /* ... */ }

```

- `S`: `Storage` _impl_ used for `Index` and `Store`
- `M`: Metadata used as generic parameter to the `MetaWithIdx` _struct_ (this is
  why I needed to explain how `MetaWithIdx` fits in first)
- `H`: `Hasher` _impl_ used for computing checksums
- `Idx`: Type to represent primitive used for for representing record indices (`u32`, `usize` etc.)
- `Size`: Type to represent record sizes in bytes (`u64`, `usize` etc)
- `SERP`: `SerializationProvider` _impl_ used for serializing metadata.

So here's what the `SerializationProvider` trait looks like:

```rust
use std::ops::Deref;
use serde::{Deserialize, Serialize};

/// Trait to represent a serialization provider.
pub trait SerializationProvider {
    /// Serialized bytes container.
    type SerializedBytes: Deref<Target = [u8]>;

    /// Error type used by the fallible functions of this trait.
    type Error: std::error::Error;

    /// Serializes the given value.
    fn serialize<T>(value: &T) -> Result<Self::SerializedBytes, Self::Error>
    where
        T: Serialize;

    /// Returns the number of bytes used by the serialized representation of value.
    fn serialized_size<T>(value: &T) -> Result<usize, Self::Error>
    where
        T: Serialize;

    /// Deserializes the given serialized bytes into a T instance.
    fn deserialize<'a, T>(bytes: &'a [u8]) -> Result<T, Self::Error>
    where
        T: Deserialize<'a>;
}
```

It's used to generalize over different `serde` data formats.

Now, we need a basic constructor for `Segment`:

```rust
impl<S, M, H, Idx, SERP> Segment<S, M, H, Idx, S::Size, SERP>
where
    S: Storage,
{
    pub fn new(index: Index<S, Idx>, store: Store<S, H>, config: Config<S::Size>) -> Self {
        Self {
            index,
            store,
            config,
            created_at: Instant::now(),
            _phantom_date: PhantomData,
        }
    }

    // ...
}
```

Next, we express the conditions for when a segment is _expired_ or _maxed out_.

```rust
impl<S, M, H, Idx, SERP> Segment<S, M, H, Idx, S::Size, SERP>
where
    S: Storage,
{
    // ...

    pub fn is_maxed(&self) -> bool {
        self.store.size() >= self.config.max_store_size
            || self.index.size() >= self.config.max_index_size
    }

    pub fn has_expired(&self, expiry_duration: Duration) -> bool {
        self.created_at.elapsed() >= expiry_duration
    }
}
```

Before, we proceed with storing records in our segment, we need to formalize
the byte layout for serialized records:

```
// byte layout for serialzed record
┌───────────────────────────┬──────────────────────────────┬───────────────────────────────────────────────┐
│ metadata_len: u32 [u8; 4] │ metadata: [u8; metadata_len] │ value: [u8; record_length - metadata_len - 4] │
└───────────────────────────┴──────────────────────────────┴───────────────────────────────────────────────┘
├────────────────────────────────────────── [u8; record_length] ───────────────────────────────────────────┤
```

As you can see, serialized record has the following parts:
1. `metadata_len`: The number of bytes required to represent serialized
   `metadata`. Stored as `u32` in `4` bytes.
2. `metadata`: The metadata associated with the record. Stored in
   `metadata_len` bytes.
3. `value`: The value contained in the record. Stored in the remaining
   `record_length - metadata_len - 4` bytes.

With, our byte layout ready, let's proceed with our `Segment::append` implementation:

```rust
impl<S, M, H, Idx, SERP> Segment<S, M, H, Idx, S::Size, SERP>
where
    S: Storage,
    M: Serialize,
    H: Hasher + Default,
    Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy,
    Idx: Serialize,
    SERP: SerializationProvider,
{
    /// Appends the given serialized Record bytes to the end of this Segment.
    ///
    /// This method first appends the serialized record to the underlying
    /// Store. Next, using the position and RecordHeader returned from
    /// Store::append, it creates the IndexRecord. Finally, it appends the
    /// IndexRecord to the Index.
    ///
    /// Returns the index Idx at which the serialized record was written.
    async fn append_serialized_record<XBuf, X, XE>(
        &mut self,
        stream: X,
    ) -> Result<Idx, SegmentOpError<S, SERP>>
    where
        XBuf: Deref<Target = [u8]>,
        X: Stream<Item = Result<XBuf, XE>> + Unpin,
    {
        let write_index = self.index.highest_index();

        let remaining_store_capacity = self.config.max_store_size - self.store.size();

        let append_threshold = remaining_store_capacity + self.config.max_store_overflow;

        let (position, record_header) = self
            .store
            .append(stream, Some(append_threshold))
            .await
            .map_err(SegmentError::StoreError)?;

        let index_record = IndexRecord::with_position_and_record_header(position, record_header)
            .ok_or(SegmentError::InvalidIndexRecordGenerated)?;

        self.index
            .append(index_record)
            .await
            .map_err(SegmentError::IndexError)?;

        Ok(write_index)
    }

    /// Appends the given Record at the end of this Segment.
    ///
    /// Returns the index at which the Record was written.
    pub async fn append<XBuf, X, XE>(
        &mut self,
        record: Record<M, Idx, X>,
    ) -> Result<Idx, SegmentOpError<S, SERP>>
    where
        XBuf: Deref<Target = [u8]>,
        X: Stream<Item = Result<XBuf, XE>> + Unpin,
    {
        if self.is_maxed() {
            return Err(SegmentError::SegmentMaxed);
        }

        // validates the append index in record metadata
        let metadata = record
            .metadata
            .anchored_with_index(self.index.highest_index())
            .ok_or(SegmentOpError::<S, SERP>::InvalidAppendIdx)?;

        let metadata_bytes =
            SERP::serialize(&metadata).map_err(SegmentError::SerializationError)?;

        let metadata_bytes_len: u32 = metadata_bytes
            .len()
            .try_into()
            .map_err(|_| SegmentError::UsizeU32Inconvertible)?;

        let metadata_bytes_len_bytes =
            SERP::serialize(&metadata_bytes_len).map_err(SegmentError::SerializationError)?;

        // sum type over two types of slices (value slice and metadata slice)
        enum SBuf<XBuf, YBuf> {
            XBuf(XBuf),
            YBuf(YBuf),
        }

        impl<XBuf, YBuf> Deref for SBuf<XBuf, YBuf>
        where
            XBuf: Deref<Target = [u8]>,
            YBuf: Deref<Target = [u8]>,
        {
            type Target = [u8];

            fn deref(&self) -> &Self::Target {
                match &self {
                    SBuf::XBuf(x_buf) => x_buf.deref(),
                    SBuf::YBuf(y_buf) => y_buf.deref(),
                }
            }
        }

        // start the stream with metadata
        let stream = futures_lite::stream::iter([
            Ok(SBuf::YBuf(metadata_bytes_len_bytes)),
            Ok(SBuf::YBuf(metadata_bytes)),
        ]);

        // chain the value to the end of the stream
        let stream = stream.chain(
            record
                .value
                .map(|x_buf| x_buf.map(|x_buf| SBuf::XBuf(x_buf))),
        );

        self.append_serialized_record(stream).await
    }
}
```

That looks more involved than it actually is. Still, let's go through it once:

- `append()`
    - Validate the append index, and obtain it if not provided.
    - Serialize the metadata, specifically the `MetaWithIdx` instance in the
      `Record`
    - Find the length of the serialized `metadata_bytes` as
      `metadata_bytes_len`
    - Serialze the `metadata_bytes_len` to `metadata_bytes_len_bytes`
    - Create a sum type to generalize over serialized byte slices and record
      value byte slices
    - Chain the byte slices in a stream in the order
      `[metadata_bytes_len_bytes, metadata_bytes, ...record.value]`
    - Call `append_serialized_record()` on the final chained stream of slices

- `append_serialized_record()`
    - Copy current `highest_index` to `write_index`
    - Obtain the `remaining_store_capacity` using the expression
      `config.max_store_size - store.size()`
    - `append_threshold` is then the remaining capacity along with overflow
      bytes allowed, i.e. `remaining_store_capacity +
      config.max_store_overflow`
    - Append the serialized stream of slices to the underlying `Store` instance
      with the computed `append_threshold`
    - Using the `(position, index_record)` obtained from `store.append()`, we
      create the `IndexRecord`
    - Append the `IndexRecord` to the underlying `Index` instance.
    - Return the `index` at which the serialized record was written, (return
      `write_index`)

Next, we implement the `AsyncIndexedRead` _trait_ for `Segment` using the same
byte layout:

```rust
#[async_trait(?Send)]
impl<S, M, H, Idx, SERP> AsyncIndexedRead for Segment<S, M, H, Idx, S::Size, SERP>
where
    S: Storage,
    S::Content: SplitAt<u8>, // enables S::Content, a byte slice, to be split
    SERP: SerializationProvider,
    H: Hasher + Default,
    Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy,
    Idx: Serialize + DeserializeOwned,
    M: Serialize + DeserializeOwned,
{
    type ReadError = SegmentOpError<S, SERP>;

    type Idx = Idx;

    type Value = Record<M, Idx, S::Content>;

    fn highest_index(&self) -> Self::Idx {
        self.index.highest_index()
    }

    fn lowest_index(&self) -> Self::Idx {
        self.index.lowest_index()
    }

    async fn read(&self, idx: &Self::Idx) -> Result<Self::Value, Self::ReadError> {
        // obtain the IndexRecord from the Index using the given Record index
        let index_record = self
            .index
            .read(idx)
            .await
            .map_err(SegmentError::IndexError)?;

        let position = S::Position::from_u64(index_record.position as u64)
            .ok_or(SegmentError::IncompatiblePositionType)?;

        // read the record bytes from the store uwing the IndexRecord
        let record_content = self
            .store
            .read(&position, &index_record.into())
            .await
            .map_err(SegmentError::StoreError)?;

        let metadata_bytes_len_bytes_len =
            SERP::serialized_size(&0_u32).map_err(SegmentError::SerializationError)?;

        // split out the bytes representing metadata_bytes_len from the record bytes
        let (metadata_bytes_len_bytes, metadata_with_value) = record_content
            .split_at(metadata_bytes_len_bytes_len)
            .ok_or(SegmentError::RecordMetadataNotFound)?;

        let metadata_bytes_len: u32 = SERP::deserialize(&metadata_bytes_len_bytes)
            .map_err(SegmentError::SerializationError)?;

        let metadata_bytes_len: usize = metadata_bytes_len
            .try_into()
            .map_err(|_| SegmentError::UsizeU32Inconvertible)?;

        // Split out the metadata_bytes from the remainder of record bytes using
        // metadata_bytes_len. The remaining bytes represent the value.
        let (metadata_bytes, value) = metadata_with_value
            .split_at(metadata_bytes_len)
            .ok_or(SegmentError::RecordMetadataNotFound)?;

        let metadata =
            SERP::deserialize(&metadata_bytes).map_err(SegmentError::SerializationError)?;

        Ok(Record { metadata, value })
    }
}
```

Again, let's summarize, what's happening above:
- Read the `IndexRecord` at the given index `idx` from the underlying `Index`
  instance
- Read the serialized record bytes using the `IndexRecord` from the underlying
  `Store` instance.
- Split and deserialize the serialized record bytes to `metadata_bytes_len`,
  `metadata` and record `value`
- Returns a `Record` instance containing the read `metadata` and `value`.

>`Segment::append` and the `AsyncIndexedRead` _trait impl_ form the majority of
>the responsiblities of a `Segment`.

Next, we need to provide an API for managing `Index` caching on `Segment`
instances:

```rust
impl<S, M, H, Idx, SERP> Segment<S, M, H, Idx, S::Size, SERP>
where
    S: Storage,
    SERP: SerializationProvider,
    Idx: Unsigned + FromPrimitive + Copy + Eq,
{
    pub async fn cache_index(&mut self) -> Result<(), SegmentError<S::Error, SERP::Error>> {
        self.index.cache().await.map_err(SegmentError::IndexError)
    }

    pub fn take_cached_index_records(&mut self) -> Option<Vec<IndexRecord>> {
        self.index.take_cached_index_records()
    }

    pub fn cached_index_records(&self) -> Option<&Vec<IndexRecord>> {
        self.index.cached_index_records()
    }
}
```

As you can see, it simply exposes the caching api of the underlying `Index`.

When constructing our `Segment`, most of the times we will need to read
the `Segment` with a given `base_index` from some storage media.
Ideally we want a mechanism that allows us to:
- Find the base indices of all the segments stored in some storage media
- Given a `base_index`, get the `Storage` _trait_ impl. instances associated
  with the `Segment` having that `base_index`

Now a `Segment` contains an `Index` and `Store`. Each of them have distinct
underlying `Storage` trait impl. instances associated with them. However, they
are still part of the same unit.

Let's create a _struct_ `SegmentStorage` to express ths notion:

```rust
pub struct SegmentStorage<S> {
    pub store: S, /// Storage associated with the store
    pub index: S, /// Storage associated with the index
}
```

Now, let's express our notion of the storage media that provides
`SegmentStorage` instances:

```rust
/// Provides SegmentStorage for the Segment with the given base_index from some storage media.
#[async_trait(?Send)]
pub trait SegmentStorageProvider<S, Idx>
where
    S: Storage,
{
    /// Obtains the base indices of the segments stored.
    ///
    /// Returns a Vec of Idx base indices.
    async fn obtain_base_indices_of_stored_segments(&mut self) -> Result<Vec<Idx>, S::Error>;

    /// Obtains the SegmentStorage for the Segment with the given idx as their base_index.
    async fn obtain(&mut self, idx: &Idx) -> Result<SegmentStorage<S>, S::Error>;
}
```

We rely on the `SegmentStorageProvider` for allocating files or other storage
units for our `Segment` instances. The receivers are `&mut` since the
operations presented here might need to manipulate the underlying storage
media.

With `SegmentStorageProvider`, we can completely decouple storage media from
our `Segment`, and by extension, our `SegmentedLog` implementation.

Now let's go back to our `Segment`. Let's create a `Segment` constructor that
uses the `SegmentStorageProvider`:

```rust
impl<S, M, H, Idx, SERP> Segment<S, M, H, Idx, S::Size, SERP>
where
    S: Storage,
    H: Default,
    Idx: Unsigned + FromPrimitive + Copy + Eq,
    SERP: SerializationProvider,
{
    /// Creates a Segment given a SegmentStorageProvider &mut ref, segment
    /// Config, base_index and a flag cache_index_records which decides
    /// whether to cache the Segment index at initialization.
    ///
    /// This function uses the SegmentStorageProvider ref to obtain the
    /// SegmentStorage associated with the Segment having the given 
    /// base_index. Next, it creates the Segment using the obtained
    /// SegmentStorage and base_index.
    ///
    /// The cache_index_records flag decides whether to read all the
    /// IndexRecord instances stored in at the associated Index at 
    /// the start. It behaves as follows:
    /// - true: Read all the IndexRecord instances into the cached vector 
    /// of IndexRecord instances in the Index
    /// - false: No IndexRecord instances are read at this moment and the 
    /// Index is not cached. The Index can later be cached with the 
    /// Segment's index caching API.
    ///
    /// Returns the created Segment instance.
    pub async fn with_segment_storage_provider_config_base_index_and_cache_index_records<SSP>(
        segment_storage_provider: &mut SSP,
        config: Config<S::Size>,
        base_index: Idx,
        cache_index_records: bool,
    ) -> Result<Self, SegmentError<S::Error, SERP::Error>>
    where
        SSP: SegmentStorageProvider<S, Idx>,
    {
        let segment_storage = segment_storage_provider
            .obtain(&base_index)
            .await
            .map_err(SegmentError::StorageError)?;

        let index = if cache_index_records {
            Index::with_storage_and_base_index(segment_storage.index, base_index).await
        } else {
            Index::with_storage_index_records_option_and_validated_base_index(
                segment_storage.index,
                None,
                base_index,
            )
        }
        .map_err(SegmentError::IndexError)?;

        let store = Store::<S, H>::new(segment_storage.store);

        Ok(Self::new(index, store, config))
    }
}
```
Next, we utilize the `SegmentStorageProvider` to provide an API to flush data
written in a `Segment` to the underlying storage media. The main idea behind
flushing is to close and reopen the underlying storage handles. This method is
generally a consistent method of flushing data across different storage
platforms. We implement this as follows:

```rust
impl<S, M, H, Idx, SERP> Segment<S, M, H, Idx, S::Size, SERP>
where
    S: Storage,
    H: Default,
    Idx: Unsigned + FromPrimitive + Copy + Eq,
    SERP: SerializationProvider,
{
    /// Flushes this Segment with the given SegmentStorageProvider.
    ///
    /// Consumes self and returns a flushed Segment.
    pub async fn flush<SSP>(
        mut self,
        segment_storage_provider: &mut SSP,
    ) -> Result<Self, SegmentError<S::Error, SERP::Error>>
    where
        SSP: SegmentStorageProvider<S, Idx>,
    {
        // back up the Index base_index and the cached_index_records
        let base_index = *self.index.base_index();
        let cached_index_records = self.index.take_cached_index_records();

        // close underlying storage handles; this flushes data
        self.index.close().await.map_err(SegmentError::IndexError)?;
        self.store.close().await.map_err(SegmentError::StoreError)?;

        // re-open and obtain storage handles for index and store
        let segment_storage = segment_storage_provider
            .obtain(&base_index)
            .await
            .map_err(SegmentError::StorageError)?;

        // reuse the previously backed up cached_index_records when
        // creating the Index from the re-opened storage handle
        self.index = Index::with_storage_index_records_option_and_validated_base_index(
            segment_storage.index,
            cached_index_records,
            base_index,
        )
        .map_err(SegmentError::IndexError)?;

        // create the store
        self.store = Store::<S, H>::new(segment_storage.store);

        // return the "flushed" segment
        Ok(self)
    }
}
```
Finally, we implement `AsyncTruncate` and `AsyncConsume` for our `Segment`:

```rust
#[async_trait(?Send)]
impl<S, M, H, Idx, SERP> AsyncTruncate for Segment<S, M, H, Idx, S::Size, SERP>
where
    S: Storage,
    Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy,
    SERP: SerializationProvider,
{
    type Mark = Idx;

    type TruncError = SegmentError<S::Error, SERP::Error>;

    async fn truncate(&mut self, mark: &Self::Mark) -> Result<(), Self::TruncError> {
        // obtain the index record for the Record at the given index
        let index_record = self
            .index
            .read(mark)
            .await
            .map_err(SegmentError::IndexError)?;

        // obtain the position of the record on the underlying store
        let position = S::Position::from_u64(index_record.position as u64)
            .ok_or(SegmentError::IncompatiblePositionType)?;

        self.store
            .truncate(&position)
            .await
            .map_err(SegmentError::StoreError)?;

        self.index
            .truncate(mark)
            .await
            .map_err(SegmentError::IndexError)?;

        Ok(())
    }
}

#[async_trait(?Send)]
impl<S, M, H, Idx, SERP> AsyncConsume for Segment<S, M, H, Idx, S::Size, SERP>
where
    S: Storage,
    SERP: SerializationProvider,
{
    type ConsumeError = SegmentError<S::Error, SERP::Error>;

    async fn remove(self) -> Result<(), Self::ConsumeError> {
        self.store
            .remove()
            .await
            .map_err(SegmentError::StoreError)?;

        self.index
            .remove()
            .await
            .map_err(SegmentError::IndexError)?;

        Ok(())
    }

    async fn close(self) -> Result<(), Self::ConsumeError> {
        self.store.close().await.map_err(SegmentError::StoreError)?;
        self.index.close().await.map_err(SegmentError::IndexError)?;
        Ok(())
    }
}
```

#### `SegmentedLog` (_struct_)

With our underlying components in place, we are ready to encapsulate the
_segmented-log_ data-structure.

Similar to `Segment`, we need to represent the configuration schema of
our `SegmentedLog` first:

```rust
/// Configuration for a SegmentedLog
#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Config<Idx, Size> {
    /// Number of read Segment instances that can have their
    /// Index cached at the same time
    ///
    /// None value indicates that all segments have their Index
    /// cached
    pub num_index_cached_read_segments: Option<usize>,

    /// Configuration for each Segment in a SegmentedLog.
    pub segment_config: segment::Config<Size>,

    /// Index to be used as the base_index of the first Segment,
    /// in case no Segment instances are already associated 
    /// with the SegmentedLog in question.
    pub initial_index: Idx,
}
```

Next, we express our notion of a _segmented-log_ as the `SegmentedLog`
_struct_:

```rust
pub struct SegmentedLog<S, M, H, Idx, Size, SERP, SSP, C> {
    /// Current write_segment where all the writes go
    write_segment: Option<Segment<S, M, H, Idx, Size, SERP>>,

    /// Vector of read_segments, sorted by base_index
    read_segments: Vec<Segment<S, M, H, Idx, Size, SERP>>,

    config: Config<Idx, Size>,

    /// Cache of segments that are currently cached
    segments_with_cached_index: Option<C>,

    /// Abstraction over storage media to acquire
    /// SegmentStorage
    segment_storage_provider: SSP,
}
```

The generic parameters are as follows:
- `S`: `Storage` _trait_ impl. to be used as storage foor underlying `Segment` instances
- `M`: Metadata to be used as parameter to `MetaWithIdx` in every `Record`
- `Idx`: Unsigned integer type to be used as record indices
- `Size`: Unsigned integer type to be used as storage size
- `SERP`: `SerializationProvider` _trait_ impl.
- `SSP`: `SegmentStorageProvider` _trait_ impl.
- `C`: `Cache` _trait_ impl.

The `Cache` _trait_ is from the crate
[generational-cache](https://github.com/arindas/generational-cache/). It
represents an abstract `Cache`, and is defined as follows:

```rust
/// A size bounded map, where certain existing entries are evicted to make space for new
/// entires.
pub trait Cache<K, V> {
    type Error;

    fn insert(&mut self, key: K, value: V) -> Result<Eviction<K, V>, Self::Error>;

    fn remove(&mut self, key: &K) -> Result<Lookup<V>, Self::Error>;

    /// Removes enough blocks that are due to be evicted to fit to the given capacity;
    /// shrinks underlying memory and capacity to the given capacity.
    ///
    /// If current length is less than or equal to the given capacity, no blocks are
    /// removed. Only the backing memory and capacity are shrunk.
    fn shrink(&mut self, new_capacity: usize) -> Result<(), Self::Error>;

    /// Reserves enough memory and increases capacity to contain the given additional
    /// number of blocks.
    fn reserve(&mut self, additional: usize) -> Result<(), Self::Error>;

    fn query(&mut self, key: &K) -> Result<Lookup<&V>, Self::Error>;

    fn capacity(&self) -> usize;

    fn len(&self) -> usize;

    fn is_maxed(&self) -> bool {
        self.len() == self.capacity()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn clear(&mut self) -> Result<(), Self::Error>;
}

/// An evicted value from cache.
#[derive(Debug, PartialEq, Eq)]
pub enum Eviction<K, V> {
    Block { key: K, value: V },
    Value(V),
    None,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Lookup<V> {
    Hit(V),
    Miss,
}
```

Now this is all fine and dandy but you are probably wondering, "Why do we need
a cache again?" Remember that if all `Segment` instances are `Index` cached, for
every `1GB` of record data, we need `16MB` of heap memory if record sizes are
`1KB`. So we made `Index` caching optional to keep memory usage from exploding.

How do we decide which `Segment` instances are to cache their Index? We use
another cache `segments_with_cached_index` to decide which Segment instances
cache their `Index`. We can choose the cache type based on a access
patterns (LRU, LFU etc.)

However, there might be cases, where the user might want all `Segment` instances
to cache their `Index`. So we also make `segments_with_cached_index` optional.

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
