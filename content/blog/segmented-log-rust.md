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

They log indices effectively behave as Lamport clocks[^1].





## Segmented Log 🪚🪵
### Introduction
### Original description in the Apache Kafka paper

## A `segmented_log` implementation
### Implementation strategy
### Attempt `#1`: Direct attempt to translate theory
### Attempt `#2`: Generic w.r.t async runtime and storage


## Conclusion

This concludes the implementation.

## References

{% references() %}

Lamport, Leslie. "Time, clocks, and the ordering of events in a distributed system." *Concurrency: the Works of Leslie Lamport.* 2019. 179-196. [https://dl.acm.org/doi/pdf/10.1145/359545.359563](https://dl.acm.org/doi/pdf/10.1145/359545.359563)

{% end %}

[^1]: A lamport clock is a logical counter to establish causality between two events. Since it's decoupled from wall-clock time, it used in distributed-systems for ordering events.
