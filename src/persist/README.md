# Materialize Dataplane Persistence

This is an in-progress implementation for a general persistence abstraction for
Materialize.

NB: This is written assuming a finished final product. Bits of this do not exist
in the code yet.

## Jargon

- _record_: A differential dataflow `((key, value), timestamp, diff)` tuple.
  Persistence assumes that the data field has a (key, value) structure, though
  the value can be empty: `()`. NB: The code currently calls this "update" in
  some places and "entry" in others. We should update those.
- _stream_: A differential dataflow "collection" of records. Also roughly
  corresponds to a timely dataflow "stream" of records. TODO: Rename this to
  collection?
- _persisted stream_: The above, but durably persisted to be replay-able across
  restarts. Indexed access is available for the persisted records.
- _(external) stream name_: A human-readable, process-unique `&str` identifier
  for a persisted stream. Used to align persisted records across restarts. Also
  used to allow read-only "tail"ing of an active persisted stream. Internally,
  this stream name gets transparently mapped to a smaller internal stream id.
- _blob_: A durable `&str` -> `[]byte` key value store. Persistence is
  written in terms of a generic `Blob` and `Buffer` interface. Examples of blob
  implementations include files in a folder and S3 (or other cloud storage).
- _buffer_: A durable log with `[]byte` entries. Persistence is written in terms
  of a generic `Blob` and `Buffer` interface. Examples of buffer implementations
  include a traditional write-ahead log (possibly on EBS or similar) and Kafka.
  TODO: Rename this to something better. Log?
- _runtime_: The internally managed threads, doing the work of persisting new
  records, compacting previously written ones, and managing read handles to
  persistent data. Accessed externally via a thread-safe and clone-able
  "client".

## External API

A persistence user initializes a `Buffer` and a `Blob` implementation and uses
them to start the runtime, receiving a `RuntimeClient` in response. This client
is the primary external interface to the persistence system. This runtime allows
us to funnel everything a process is persisting through a single place for
batching and rate limiting. The buffer and blob are exclusively locked for
writes by this process (they can be opened for reads by other processes,
however).

The runtime stops when every copy of the client has been dropped (or when any
client's stop method has been called), attempting to first gracefully finish any
pending work, but rejecting new work.

As needed, the user creates a persisted stream by name, returning a token. At
dataflow construction time, this token can be exchanged to create one of several
available persistence operators (see below). When the operator is run, it hands
any blocking io or cpu heavy work to the runtime, asking the dataflow to be
scheduled again when this finishes. If the same buffer and blob implementations
were previously used to persist a stream with the same name, this previous data
is read and also output by the operator, yielding periodically to allow
downstream operators a chance to process it.

## Operator Guarantees

- _idempotence_: Any record emitted by a persistence operator is guaranteed to
  be included in the output of all future operators for the same persistent
  stream. This is true even before the record's timestamp has been closed.
- _ordering_: The same guarantees you'd expect out of any differential
  collection or timely stream. TODO: Is there anything more specific to say
  here?
- _determinism_: TODO: This is easy in the absence of errors from the storage,
  but that's not the interesting part. "S3 is down" is obviously a transient
  error, but even things like permissions errors are transient in terms of the
  strict definition of deterministic we're using. I think the Materialize "error
  stream" model is potentially a good way to model this where anything on the
  main output is deterministic at some timestamp iff the error stream is an
  empty collection at that timestamp. Think about this more.

## Example: Persisting a Stream

NB: This operator doesn't exist yet, but this is more or less what the interface
will look like.

```rust
let persister: RuntimeClient = ...
let token = persister.create_or_load("example stream")?;

let stream: Stream<(K, V, T, R)> = ...
let (persisted_stream, err_stream) = stream.new_persisted_stream(token);
```

Whenever the operator is woken up to do work, it sends off any new inputs to be
persisted. Any previous inputs that have finished being durably written are
forwarded on as an operator output. When the input frontier advances, the output
fronter is advanced to match, but only after all relevant input data has been
persisted.

Any errors encountered while persisting (storage unavailable, etc) are emitted
to an error stream.

A future version of the persist crate will also return a metadata handle from
`new_persisted_stream` that allows for indexed access into the data that has
been persisted. This is modeled after the `arrange` operator in differential
dataflow.

## Example: Persisting an Input

```rust
let persister: RuntimeClient = ...
let token = p.create_or_load("example input")?;

let (mut handle, cap) = worker.dataflow(|scope| {
    let (input, stream) = scope.new_persistent_unordered_input(token);
    construct_the_rest_of_the_dataflow(stream);
    input
});
let (tx, rx) = mpsc::channel();
let mut session = handle.session(cap);
for i in 1..=5 {
    session.give((i.to_string(), i, 1), tx.into());
}
for i in 1..=5 {
    if let Err(err) = rx.recv()? {
        handle_err_persisting_input(err);
    }
}
```

Dataflow construction returns an input handle, which can be used to insert data
into the dataflow. Whenever the operator is woken up to do work, it sends off
any new inputs to be persisted. Any previous inputs that have finished being
durably written are forwarded on as an operator output. These writes are also
acknowledged to the callback/channel that they arrived with, in case some work
needs to be blocked on their durability (in Materialize's case, the coordinator
waits to acknowledge an INSERT/UPDATE/DELETE until that data is replayable.)
When the input frontier advances, the output fronter is advanced to match, but
only after all relevant input data has been persisted.

Any errors encountered while persisting new inputs (storage unavailable, etc)
are exposed via the callback/channel. Errors encountered while replaying
previously persisted records are emitted on the error output.

## Example: A Persistent Sink

```rust
let persister: RuntimeClient = ...
let token = persister.create_or_load("example sink")?;

let stream: Stream<(K, V, T, R)> = ...
let err_stream = stream.new_persisted_sink(token);
```

A single input operator that persists its input. Any errors encountered while
persisting (storage unavailable, etc) are emitted to the error stream.

## Example: A Persistent Source

```rust
let client: ReadOnlyClient = ...
let token = client.load_existing("name of some other stream")?;

let (stream, err_stream) = stream.new_persisted_source(token);
```

An operator that tails an active persistent stream, emitting its contents. This
can be any stream we're persisting (sources, tables, operators), not just
streams from `new_persisted_sink`.

Errors encountered while replaying previously persisted records are emitted on
the error output.

## Short-Term/Long-Term

Timely and differential dataflows like to be perfectly deterministic. Real world
sources have transient errors and garbage collection and are not. Currently,
this is all smashed together. Long-term, we'd like to split what is currently
the dataflow crate into two logical units. The first part containing the messy
bits of unavailability and lost data and the second part being deterministic and
having a single correct answer for each possible set of inputs.

Persistence is well-positioned to be the boundary between these two parts. Lost
data is addressed by being very careful to only emit records that we can forever
replay. Transient errors are unavoidable, but we can translate them into
differential dataflow semantics in a single place and battle-harden it.

In a technical sense, it's even possible for this boundary to performantly be
separate differential dataflows in a process (or less performantly, in different
processes), with a persistence sink as the end of one and a persistence source
as the start of another. Unclear if that's useful.

Persistence may also be a useful building block for blue-green deployments.
Glossing over all sorts of tricky details, one could imagine a "release
qualification" mode. A binary started in this mode wouldn't accept user
requests, but instead would transparently mirror the schema of a production
cluster, tail the persisted streams for its sources, and compare its own output
against the production cluster's persisted sinks. This would allow for
performance measurements and behavior diffing without adding _any_ load to the
production deployment or production sources. In a cloud first world, we could do
this for whichever set of customers give us necessary confidence in the new
version.
