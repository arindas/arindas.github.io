+++
title = "Building Segmented Logs in Rust: From Theory to Production!"
date = 2023-08-01
description = "Explore a Rust implementation of Segmented Logs: The persistence mechanism behind message-queues and write ahead logs in databases. Embark on a journey from the original segmented log research paper to a production grade fully tested and benchmarked implementation."

[taxonomies]
tags = ["rust", "tokio", "glommio", "segmented-log", "message-queue", "distributed-systems"]

[extra]
toc = true
giscus = true
+++

## Prologue: The Log 📃🪵

First, let's clarify what we mean by a "log" in this context. Here, log
refers to an append only ordered collection of records. The records are ordered
by time (as a consequence of being appended to the log).

<p align="center">
<img src="/img/log.png" alt="queue-diagram" width="50%"/>
</p>
<p align="center" class="caption">
<b>Fig:</b> The Log: An append only ordered collection of records.
</p>

The log indices denote a notion of time since the records are ordered by time.
They can be even thought of as timestamps, with the convenient property of
being decoupled from actual wall-clock time.

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
- Apply all the operations correctly _without any random mistakes_ in the
_correct pre-determined_ way.

With computer science, we can model the students as _deterministic_ state
machines, the students' `current_number` as their internal state, and the
instructions as inputs to the state machines. From here, we can say the
following:

---

If different identical deterministic state machines start from the same initial
state, and receive the same set of inputs in the same order, they will end in
the same state.

---

#### State-machine replication

Now there's a new problem: the last two rows of students can't hear the teacher
properly. What do they do now? The teacher needs a solution that enables them
to give them the same set of instructions to the backbenchers without actually
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
then perform the calculations on their own private sheet. When they are done
writing on a sheet of paper with the instructions, they only share the sheet
containing the instructions. They never share their private sheet. After
passing a sheet full of instructions, the start writing the instructions on a
new sheet of paper.

The backbenchers are able to receive the same set of instructions in the same
sequence through the sheets. They perform the necessary calculations on their
own private sheet starting from the same initial number using the instructions
from the received sheets.

If we inspect carefully, this mechanism of sharing the instructions through the
sheets behaves like a log. The private sheets act as the internal states. The
collection of sheets collectively act as a log.

---

Now, in our case, because the backbenchers receive the same set of instructions
in the same sequence, they go through the same set of internal states in the
same sequence and arrive at the same final state. They effectively replicate
the front and middle-benchers. Since, we can model students as state machines,
we effectively did __state machine replication__ with a __log__.

---

Finally, since the backbenchers receive the instructions through the log and
not directly from the teacher, they lag behind a bit but eventually arrive at
the same results. So we can say there is a __replication lag__.

These concepts directly translate to distributed systems. Consider this:

There is a database partition in a distributed database responsible for a
certain subset of data. When any data manipulation queries are route to it, it
has to handle the queries. Instead of directly committing the effect of the
queries on the underlying storage, it first writes the operations to be applied
on the local storage, one-by-one into a log called the "write-ahead-log". Then
it applies the operations from the write ahead log to the local storage.

In case there is a database failure, it can re-apply the operations in the
write-ahead-log from the last committed entry and arrive at the same state.

When this database partition needs to replicate itself to other follower
partitions, it simply replicates the write-ahead-log to the followers instead
of copying over the entire materialized state. The followers can use the same
write ahead log to arrive to the same state as the leader partition.

Now the follower partitions, have to receive the write-ahead-log first over the
network. Only then can they apply the operations. As a result they lag behind
the leader. This is replication lag.

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

Now it's not necessary that all the requests have to be handle by the same
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

It might come as a surpise, but we have already come across a `segmented_log`
in the previous example.

### Introduction

In the previous example, the collection of sheets containing the instructions
collectively behaved as a log. We can also say that the _instruction log_ was
_segmented_ across the different sheets of paper.

Call the individual sheets of paper _segments_. The collection of sheets can
now be called a `segmented_log`.

Wait wait don't leave yet 😅. Let's take this a bit more seriously this time.

Let's go back to the log. At the end of the day a log is sequential collection
of elements. Whats the simplest data structure we can use to implement this? An
array. However, we need persistence. So let's use a file based abstraction
instead. (We can quite literally map a file to a process's virtual memory
address space using the
[`mmap`](https://man7.org/linux/man-pages/man2/mmap.2.html) system call and
then use it like an array, but that's a topic for a different day.)

<p align="center">
<img src="/img/log.png" alt="queue-diagram" width="50%"/>
</p>
<p align="center" class="caption">
<b>Fig:</b>: A log implementation based on a file.
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

## A `segmented_log` implementation
### Implementation strategy
### Attempt `#1`: Direct attempt to translate theory
### Attempt `#2`: Unify async runtimes and storage mechanisms


## Conclusion

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

{% end %}

[^1]: A lamport clock is a logical counter to establish causality between two
events. Since it's decoupled from wall-clock time, it's used in
distributed-systems for ordering events.
