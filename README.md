# loginus

## Example use case

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

* [ ] Parser for journald-export files
* [ ] ngrams
* [ ] vector stuff

...

* [ ] Use AVX and such to be fancy and fast