# Hypotheses for the server round, written before it ran

This file was committed before any server in this round was measured, as
[`HYPOTHESES.md`](HYPOTHESES.md) was for the first round. The results are
judged against it, losses included.

**An edit made after the round.** The round also ran a third-party io_uring
server as a reference, and most of the predictions below were written as
ratios to it. That server is not part of the published comparison, so the
predictions stated against it are omitted here rather than reworded after the
fact. What remains is unchanged.

## The question

Could an HTTP server built on blocking code on virtual threads (the shape a
Lark-based backend for [Pelican](https://github.com/matthewjones372/pelican)
would have) serve as fast as a server built on io_uring, on HttpArena's
HTTP/1.1 profiles?

The first round compared clients. This one compares servers, which the README
listed under "Not yet tested".

## Contestants

| Name | What it is |
|---|---|
| `vt-arena` | written for this round: a `ServerSocket`, one virtual thread per connection, blocking reads, a parser over the read buffer that allocates no strings, and every pipelined request already in the buffer answered with one write |
| `jetty-arena` | hand-written Jetty 12 core `Handler`, invoked non-blocking on the selector, with no Pelican: the floor under Pelican's current backend |

`vt-arena` is the best case for a Lark backend: Pelican and Lark would add
work on top of it, never remove any.

## Setup

One 4-core cloud VM. Each server pinned to two cores, gcannon (HttpArena's
load generator) pinned to the other two. The same request files as HttpArena,
from its `requests/` directory. Three runs per contestant and profile,
alternating contestants, median reported.

## Predictions

| # | Profile | Prediction |
|---|---|---|
| S4 | `baseline` (rotating GET, POST with `Content-Length`, chunked POST) | `jetty-arena` is **within ±20%** of `vt-arena` |

S1 to S3 were stated against the reference server and are omitted.

## What this round cannot show

- Two server cores against HttpArena's 64. Thread-per-core designs and the
  JDK's poller may scale differently from 2 to 64 cores.
- The load generator shares the machine's memory bus and kernel with the
  server.
- No TLS, compression, `async` (32,000 connections does not fit this VM) or
  CPU-per-request profile (gcannon offers no fixed rate).

## Exploratory addendum: JDK 27, written while it ran and before any result was read

Not part of the pre-registered round above. JDK 27 adds `-Djdk.pollerMode=3`, an epoll poller per carrier thread.
Contestants `vt-arena-27` (JDK 27, defaults) and `vt-arena-27-pc` (JDK 27, poller per carrier), against `vt-arena` on
JDK 25, `baseline` and `pipelined` only.

| # | Prediction |
|---|---|
| X1 | `vt-arena-27` within noise of `vt-arena` |
| X2 | `vt-arena-27-pc` gains 5–15% over `vt-arena` on `baseline`, less on `pipelined` |

A gain much larger than that would refute the expectation that the per-request system calls and parks, not cross-core
wake-ups, are most of the cost.

## Exploratory addendum: uring-arena, written before it was measured

vt-arena's handler, unchanged, on a runtime written for this: one carrier thread per core, each with its own io_uring
(single issuer, deferred task work) and its own SO_REUSEPORT listener; each connection a `jdk.internal.vm.Continuation`
whose `recv` and `send` queue a request and yield. One `io_uring_enter` per carrier loop.

Its predictions (U1 to U3) were stated against the reference server and are omitted.
