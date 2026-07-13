import { Result } from "better-result";
import type { QueueEmptyError } from "./errors/index";
import { Queue } from "./queue/index";
import { Resource } from "./resource/index";
import { Stream, pipe } from "./stream/index";
import type { StreamError, StreamValue } from "./stream/index";
import { Task } from "./task/index";
import type { Task as TaskType, TaskError, TaskSuccess } from "./task/index";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? true
    : false;
type Expect<Value extends true> = Value;

interface ReadError {
  readonly _tag: "ReadError";
  readonly path: string;
}
interface WriteError {
  readonly _tag: "WriteError";
  readonly path: string;
}

const readError: ReadError = { _tag: "ReadError", path: "input.txt" };
const writeError: WriteError = { _tag: "WriteError", path: "output.txt" };

const queueResult = Queue.unbounded<undefined>().tryTake();
export type QueueResultPreservesUndefined = Expect<
  Equal<typeof queueResult, Result<undefined, QueueEmptyError>>
>;

const composed = Task.flatMap(Task.fail(readError), () => Task.fail(writeError));
export type FlatMapUnionsErrors = Expect<
  Equal<typeof composed, TaskType<never, ReadError | WriteError>>
>;

const recovered = Task.catchTag(
  Task.from<number, ReadError | WriteError>(async () => Result.err(readError)),
  "ReadError",
  () => Task.succeed("fallback"),
);
export type CatchTagRemovesHandledError = Expect<
  Equal<typeof recovered, TaskType<number | string, WriteError>>
>;

const all = Task.all([
  Task.from<number, ReadError>(async () => Result.ok(1)),
  Task.from<string, WriteError>(async () => Result.ok("ready")),
] as const);
export type AllPreservesTuple = Expect<Equal<TaskSuccess<typeof all>, readonly [number, string]>>;
export type AllUnionsErrors = Expect<Equal<TaskError<typeof all>, ReadError | WriteError>>;

const selected = Task.select({
  count: Task.from<number, ReadError>(async () => Result.ok(1)),
  label: Task.from<string, WriteError>(async () => Result.ok("ready")),
});
export type SelectPreservesKeysAndValues = Expect<
  Equal<
    TaskSuccess<typeof selected>,
    | { readonly key: "count"; readonly value: number }
    | { readonly key: "label"; readonly value: string }
  >
>;
export type SelectUnionsErrors = Expect<Equal<TaskError<typeof selected>, ReadError | WriteError>>;

const typedStream = pipe(
  Stream.from([1, 2]),
  Stream.mapEffect((value) => Task.from<string, WriteError>(async () => Result.ok(String(value)))),
);
export type StreamMapEffectValue = Expect<Equal<StreamValue<typeof typedStream>, string>>;
export type StreamMapEffectError = Expect<Equal<StreamError<typeof typedStream>, WriteError>>;

const merged = Stream.merge(Stream.fail(readError), typedStream);
export type MergeUnionsValues = Expect<Equal<StreamValue<typeof merged>, string>>;
export type MergeUnionsErrors = Expect<Equal<StreamError<typeof merged>, ReadError | WriteError>>;

const resource = Resource.make(Task.fail(readError), () => undefined);
const used = Resource.use(resource, () => Task.fail(writeError));
export type ResourceUseUnionsErrors = Expect<Equal<TaskError<typeof used>, ReadError | WriteError>>;

// @ts-expect-error A Task must resolve to Result rather than a raw success value.
Task.run(async () => 1);

// @ts-expect-error Task.map is pure; asynchronous work must be represented with Task.flatMap.
Task.map(Task.succeed(1), async (value) => value + 1);

// @ts-expect-error Opaque Streams require the explicit Stream.from interop boundary.
Stream.runCollect([1, 2, 3]);

// @ts-expect-error The explicit Queue result cannot be treated as the old ambiguous sentinel.
const ambiguousQueueValue: undefined = queueResult;
void ambiguousQueueValue;
