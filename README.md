# loginus

This crate constitutes a collection of utility (libraries) to parse and
manipulate log streams in the Journal Export Format of journald.

## Compression

In this section, we describe the kind of experiments that we want to conduct
regarding compression.

Generally, when shipping logs, there is a trade-off between bandwidth and
getting logs in real-time: The shorter the delay, the less effective the
compression becomes as shorter segments need to be compressed individually. On
the other hand, the larger a batch, the longer the delay until a log message
appears in the monitoring system.

We are interesting the following questions:

* Compressing log entries individually using a dictionary created with
  individual log entries, what is the average compression ratio?
* What is the compression ratio for randomnly sampled subsequences of
  lengths 1, 2, 4, 8, ...?

## Similarity analysis

Given a subsequence of logs (e.g. the ones belonging to a service), provide a
normalized vector that represents that log stream.

### Example use case

Imagine you have two journals of two CI-runs of a reasonably complex system. One
run consistutes a success, the other a failure. You want to know: Which is the
service, for which the logs are most dissimilar between the successful and the
failing run.

Further, you might want to ask: which log message has the most dissimilarity
with the successful 'average' log message.

The analysis is roughly done by splitting log messages into a set of tokens.
Each token is embedded into a binary vector space (e.g. using a simple hash,
such as md5). The vectors are then added up.

The vectors thus gained can be summed up for each group (e.g. service), forming
a group vector. The similarity of those group vectors can then be compared for
each service using cosinus-similarity (hence the name.)

## Todos

* [x] Parser for journald-export files
* [ ] Compression experiments
* [ ] ngrams
* [ ] vector stuff

...

* [ ] Use AVX and such to be fancy and fast