> Turn in a document that describes the overall shape of your project. This should include the project partners, a high level description of the goals and structure of the system, identification of the key distributed systems problem in the system, what languages and resources will be necessary to carry it out, and your plan for evaluating the system. Be sure to think about what metrics you will use to evaluate the system – throughput, latency, scalability, runtime – and sketch a notional graph of how you think that metric will change as the system size or load increases. (Of course, I don’t expect you to know the actual results, but I want you to think about what you will measure and what orders of magnitude to expect.)

# Project Proposal

## Overview

- Project name: kagecal (kage bunshin no calendar) (shawdow clone calendar)
- Partners: [Leo Molina](mailto:lmolina3@nd.edu), [Sam Neisewander](mailto:sneisewa@nd.edu)
- Repository: [kagecal](https://github.com/samneisewanderND/kagecal)

### Description
Kage Bunshin no Calendar (Shadow Clone Calendar) is a decentralised calendar application where several peers can manipulate a single shared calendar timeline.

Users can add, remove, update, and view the shared calendar timeline.

## Goals & Structure

The *key disributed systems problem* in this system would be garunteeing consistency of the calendar data across all peers. For example, if a peer adds an event to the calendar, one challenge is figuring out how to disseminate that change to the rest of the peers. Things get more interesting when considering updates; if a two peers update the same event, and the updates arrive at a third and fourth peer in different orders, how can we reconcile the calendar state between all peers? We plan to approach these problems by using patterns like logical clocks and election algorithms to ensure consistent state.

We would like the client application to be able to view the calendar and search events in the calendar, but we do not plan to implement fancy features like access control and notifications, as these are not really related to the distributed systems problem.

## Languages & Resources
We plan to program the peer in `Python` and to use the `catalog.cse.nd.edu` nameserver to help connect new peers to the system. We plan to test and benchmark our system using our local machines and the student machines (no CRC or cloud compute).

## Evaluation
We plan to write correctness and persistence tests. We also plan to write some performance benchmarking tests to evauluate metrics like `throughput` (how many events we can process per unit time) and `latency` (how long it takes a single update to propogate to the whole system).

Here are some notional graphs for how we think our performance metrics will trend with respect to system size or load.

![Figure 1. Latency versus system size](https://yld.me/raw/mWcv)
Figure 1. Latency versus system size

![Figure 2. Throughput versus system size](https://yld.me/raw/qPYR
)
Figure 2. Throughput versus system size
