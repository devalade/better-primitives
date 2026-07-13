# better-primitives

Effect-shaped async primitives for TypeScript, built on `Promise`, `AbortSignal`, and
[`better-result`](https://github.com/dmmulroy/better-result). There is no Effect runtime and no
environment or Layer type.

The central contract is a lazy computation with an explicit expected-failure channel:

```ts
type Task<A, E = never> = (signal: AbortSignal) => Promise<Result<A, E>>;
```

Promise rejection is reserved for cancellation control and unrecoverable defects. Expected failures
are values in `Result.err` and remain visible in inferred types.

## Install

```sh
npm install better-primitives better-result
```

## Quick start

```ts
import { Result } from "better-result";
import { Resource, Stream, Task, Time, pipe } from "better-primitives";

interface RequestError {
  readonly _tag: "RequestError";
  readonly cause: unknown;
}

const request = (url: string) =>
  Task.tryPromise(
    async (signal) => {
      const response = await fetch(url, { signal });
      return response.text();
    },
    (cause): RequestError => ({ _tag: "RequestError", cause }),
  );

const collected = await pipe(
  Stream.from(["https://example.com", "https://example.org"]),
  Stream.mapConcurrent(request, 4),
  Stream.runCollect,
);

if (Result.isError(collected)) {
  // RequestError | Cancelled
  console.error(collected.error._tag);
} else {
  console.log(collected.value);
}
```

## Failure model

- `Task<A, E>` is lazy. Calling it directly requires an `AbortSignal`; normally use `Task.execute`,
  `Task.run`, or compose it into another primitive.
- `Task.execute(task)` returns `Promise<Result<A, E | Cancelled>>`.
- `Task.run(task)` returns a `Fiber<A, E>` whose `result()` has the same explicit cancellation union.
- `Task.tryPromise` is the boundary for translating a rejecting Promise into a typed failure.
- A rejection escaping `Task.from`, an operator callback, or a raw async iterable remains a defect.

`Task.map` is pure. Use `Task.flatMap` when the next operation is another Task:

```ts
const program = Task.flatMap(request("/profile"), (profile) => Task.succeed(profile.length));

const outcome = await Task.execute(program);
```

The main Task combinators preserve precise unions:

- `map`, `mapError`, `flatMap`, `catchAll`, and `catchTag`
- `all` for concurrent tuple results in input order
- `race` and `select` for first-success races
- `forEach` for ordered, bounded concurrent traversal

`race` and `select` ignore typed branch failures while another branch can succeed. If every branch
fails, they return the first observed typed failure. A defect rejects. Losing branches are aborted and
settled before the race completes.

## Typed streams

`Stream<A, E>` is opaque: it is not directly an `AsyncIterable`. Operators preserve or union the
expected-failure channel, and runners add `Cancelled`:

```ts
const numbers = pipe(
  Stream.from([1, 2, 3, 4]),
  Stream.filter((value) => value % 2 === 0),
  Stream.map((value) => value * 10),
);

const result = await Stream.runCollect(numbers);
// Result<ReadonlyArray<number>, Cancelled>
```

Use `mapEffect`, `mapConcurrent`, or `buffer` for Task-returning transforms. `mapConcurrent` emits in
completion order; `buffer` preserves input order. Both bound concurrency, abort owned mapper work on
failure or early return, and settle it before completing.

Interop with the raw protocol is explicit:

- `Stream.from(iterable)` lifts an `Iterable` or `AsyncIterable`.
- Cancellation closes a lifted iterator with `return()`. A raw iterator's already-running `next()`
  cannot itself be force-cancelled unless that iterator implements its own cancellation mechanism.
- `Stream.toAsyncIterable(stream)` lowers a Stream. Because raw `AsyncIterable` has no typed failure
  channel, an `E` is thrown at this boundary.

Available constructors and operators also include `fail`, `fromEvent`, `unfold`, `tick`, `take`,
`tap`, `chunks`, `merge`, and `zip`. Runners include `runCollect`, `runForEach`, and `runFold`.

## Resources and scopes

`Resource<A, E>` keeps acquisition failure typed and registers cleanup only after acquisition succeeds:

```ts
const connection = Resource.make(Task.tryPromise(openConnection, toConnectionError), (value) =>
  value.close(),
);

const result = await Task.execute(Resource.use(connection, (value) => Task.succeed(value.status)));
```

`Resource.use` always closes its fresh Scope. Cleanup rejection is a defect rather than part of the
resource's expected-failure union. Cancellation is added only when the Resource Task is executed.
`Task.fork(scope, task)` supervises a Fiber under an existing Scope.

## Time and cancellation

Time operations are Tasks:

```ts
await Task.execute(Time.sleep(100));
await Task.execute(Time.timeout(1_000, request("/health")));
```

Pass caller-owned cancellation to execution boundaries and Stream runners:

```ts
const controller = new AbortController();

const taskResult = Task.execute(program, { signal: controller.signal });
const streamResult = Stream.runCollect(numbers, { signal: controller.signal });

controller.abort("no longer needed");
```

## Modules

| Module   | Import                       | Role                                                      |
| -------- | ---------------------------- | --------------------------------------------------------- |
| Task     | `better-primitives/task`     | Typed lazy computations and Fibers                        |
| Stream   | `better-primitives/stream`   | Typed async streams, operators, and runners               |
| Resource | `better-primitives/resource` | Typed acquisition and deterministic release               |
| Scope    | `better-primitives/scope`    | Structured lifetime and finalizers                        |
| Time     | `better-primitives/time`     | Sleep, timeout, and deadline Tasks                        |
| Queue    | `better-primitives/queue`    | Backpressure queues                                       |
| Channel  | `better-primitives/channel`  | MPSC, oneshot, broadcast, and watch channels              |
| Sync     | `better-primitives/sync`     | Mutex, Semaphore, Deferred, Latch, and related primitives |
| Cancel   | `better-primitives/cancel`   | AbortSignal trees                                         |
| Errors   | `better-primitives/errors`   | Cancellation, timeout, and queue error values             |

`Queue.tryTake()` returns `Result<A, QueueEmptyError>`, so `undefined` remains a valid queued value.

The public `Fiber`, `Scope`, and synchronization contracts use `Disposable` and `AsyncDisposable`.
TypeScript consumers that disable `skipLibCheck` should include `ESNext.Disposable` in
`compilerOptions.lib`.

## Development

This repository declares pnpm 11.12.0:

```sh
pnpm install --frozen-lockfile
pnpm check
pnpm test
pnpm build
```

## Benchmark

The benchmark compares reusable lazy programs in better-primitives and Effect 3.22.0. It uses
`Task.execute` and `Effect.runPromiseExit` so both library rows preserve a typed outcome at the
execution boundary. Native Promise and AsyncIterable rows are lower-bound baselines: they do not
provide an equivalent typed failure cause, effect runtime, or structured-concurrency semantics.

```sh
pnpm bench
pnpm bench -- --quick
pnpm bench -- --json
```

The suite covers success, expected failure, ten-step `map` and `flatMap` pipelines, concurrent
collection of 100 successful computations, and stream mapping/collection over 1,000 values. Programs
are checked for equivalent results before measurement, warmed up independently, sampled in rotating
order, and reported as median operations per second relative to Effect.

The `all` scenario sets Effect concurrency to `"unbounded"` to match `Task.all`'s concurrent contract.
That case therefore includes Effect's fiber scheduling and supervision costs; it is not a comparison
against Effect's sequential collection mode.

Timing can be tuned with `BENCH_WARMUP_MS`, `BENCH_SAMPLE_MS`, and `BENCH_SAMPLES`. Run comparisons on
an otherwise idle machine and treat results as machine- and version-specific rather than universal.
