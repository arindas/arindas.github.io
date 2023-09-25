+++
title = "Building Segmented Logs in Rust: From Theory to Production!"
date = 2023-08-01
description = "Explore a Rust implementation of the persistence mechanism behind message-queues and write-ahead-logs in databases. Embark on a journey from the original segmented log research paper to a production grade fully tested and benchmarked implementation."
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

This section presents two implementations of the `segmented_log`. The second
implementation was made to overcome the limitations of the first one.

### Attempt `#1`: Direct implementation based on the Kafka paper

The code for this section can be found at
[laminarmq@ed4beea/src/commit_log](https://github.com/arindas/laminarmq/tree/ed4beea210b3ae6174959935c595f4f53d437ac7/src/commit_log)

#### Implementation outline

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

### Attempt `#2`: Support streaming writes, decouple persistence

The code for this section can be found at
[laminarmq@e06aa58/src/storage](https://github.com/arindas/laminarmq/tree/e06aa58256a509444f4144b1d1c236587e075764/src/storage)<br/>
The benchmarks can be found at
[laminarmq@e06aa58/benches](https://github.com/arindas/laminarmq/tree/e06aa58256a509444f4144b1d1c236587e075764/benches)

#### Implementation outline

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
