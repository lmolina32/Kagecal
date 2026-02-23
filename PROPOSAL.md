> Turn in a document that describes the overall shape of your project. This should include the project partners, a high level description of the goals and structure of the system, identification of the key distributed systems problem in the system, what languages and resources will be necessary to carry it out, and your plan for evaluating the system. Be sure to think about what metrics you will use to evaluate the system – throughput, latency, scalability, runtime – and sketch a notional graph of how you think that metric will change as the system size or load increases. (Of course, I don’t expect you to know the actual results, but I want you to think about what you will measure and what orders of magnitude to expect.)

# Project Proposal

## Overview

- Project name: kagecal (kage bunshin no calendar) (shawdow clone calendar)
- Partners: [Leo Molina](mailto:lmolina3@nd.edu), [Sam Neisewander](mailto:sneisewa@nd.edu)
- Repository: [kagecal](https://github.com/samneisewanderND/kagecal)

### Description
TODO
decentralised calendar application

## Goals & Structure
TODO
- questions:
    - should we allow overlapping events?
    - should we enforce access control?
    - are events replicated across machines (consistency problem) or are events distributed across machines (responsibility problem)

- calendar with operations:
    - add event: 
    - remove own event: 
    - modify own event
    - query (current event, past event, future events)

- optional features:
    - search / filter
    - notifications

## Languages & Resources
TODO
- python, local machines and student machines
- catalog.cse.nd.edu

## Evaluation
TODO: elaborate on this stuff
- correctness
- persistance
- throughput
- latency
- notional graphs (can just upload images to yld.me and insert via markdown)