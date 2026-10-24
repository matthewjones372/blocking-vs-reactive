# Hypotheses for the server round, written before it ran

This file was committed before any server in this round was measured, as
[`HYPOTHESES.md`](HYPOTHESES.md) was for the first round. The results are
judged against it, losses included.

## The question

Could an HTTP backend built on blocking code on virtual threads (the shape a
Lark-based backend for [Pelican](https://github.com/matthewjones372/pelican)
would have) beat [Ktron](https://github.com/MDA2AV/Ktron), an io_uring server
with one reactor per core, on HttpArena's HTTP/1.1 profiles?

The first round compared clients. This one compares servers, which the README
listed under "Not yet tested".

## What was already known when this was written

To be honest about what these predictions were anchored on: before writing
them, the author had read HttpArena's published results (64-core host). There,
Helidon SE on Níma, a virtual-thread-per-connection blocking server, reaches
41% of Ktron's rps on `baseline`, 22% on `pipelined`, 39% on `json-comp` and
22% on `limited-conn`. It spends 2.3 times Ktron's CPU per request on
`latency-1m`. Those numbers are another team's server on other hardware. This
round asks whether a server written for the purpose, with no framework, does
better.

## Contestants

| Name | What it is |
|---|---|
| `ktron` | HttpArena's own Ktron entry (`frameworks/ktron`), built from source, unchanged |
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
| S1 | `baseline` (rotating GET, POST with `Content-Length`, chunked POST) | `vt-arena` reaches **30–60%** of `ktron`'s rps |
| S2 | `pipelined` (16 deep) | `vt-arena`'s ratio to `ktron` is **at least its S1 ratio**: answering a batch with one write recovers what batching gives Ktron |
| S3 | `/json/50`, no compression | `vt-arena`'s ratio to `ktron` is **at least its S1 ratio**: serialisation costs both about the same |
| S4 | `baseline` | `jetty-arena` is **within ±20%** of `vt-arena` |

## What decides the question

- **S1 at 70% or more**: a Lark backend could plausibly approach Ktron, and
  is worth a Pelican spec on performance grounds.
- **S1 below 50%**: a blocking backend cannot beat Ktron. A Lark backend's case
  is then against Pelican's Jetty backend (S4), not against Ktron.
- **Between the two**: inconclusive at this scale. Rerun on dedicated hardware
  before deciding.

## What this round cannot show

- Two server cores against HttpArena's 64. Thread-per-core designs and the
  JDK's poller may scale differently from 2 to 64 cores.
- The load generator shares the machine's memory bus and kernel with the
  server.
- No TLS, compression, `async` (32,000 connections does not fit this VM) or
  CPU-per-request profile (gcannon offers no fixed rate).
