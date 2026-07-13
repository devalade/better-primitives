import { createRequire } from "node:module";
import { performance } from "node:perf_hooks";
import process from "node:process";

import { Result } from "better-result";
import { Cause, Chunk, Effect, Exit, Option, Stream as EffectStream } from "effect";

import { Stream, Task, pipe } from "../dist/index.mjs";

const require = createRequire(import.meta.url);
const { version: effectVersion } = require("effect/package.json");
const args = new Set(process.argv.slice(2));
const quick = args.has("--quick");
const json = args.has("--json");

const defaults = quick
  ? { samples: 3, sampleMs: 100, warmupMs: 50 }
  : { samples: 5, sampleMs: 250, warmupMs: 150 };

function readPositiveNumber(name, fallback) {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) {
    throw new RangeError(`${name} must be a positive number`);
  }
  return value;
}

function readPositiveInteger(name, fallback) {
  const value = readPositiveNumber(name, fallback);
  if (!Number.isInteger(value)) throw new RangeError(`${name} must be an integer`);
  return value;
}

const config = {
  samples: readPositiveInteger("BENCH_SAMPLES", defaults.samples),
  sampleMs: readPositiveNumber("BENCH_SAMPLE_MS", defaults.sampleMs),
  warmupMs: readPositiveNumber("BENCH_WARMUP_MS", defaults.warmupMs),
};

let blackhole;

async function measure(operation, durationMs) {
  const measurementBatchSize = 16;
  const startedAt = performance.now();
  let iterations = 0;
  let elapsed = 0;

  do {
    for (let index = 0; index < measurementBatchSize; index += 1) {
      blackhole = await operation();
    }
    iterations += measurementBatchSize;
    elapsed = performance.now() - startedAt;
  } while (elapsed < durationMs);

  return (iterations * 1_000) / elapsed;
}

function median(values) {
  const ordered = [...values].sort((left, right) => left - right);
  const middle = Math.floor(ordered.length / 2);
  return ordered.length % 2 === 0 ? (ordered[middle - 1] + ordered[middle]) / 2 : ordered[middle];
}

function relativeSpread(values, center) {
  return ((Math.max(...values) - Math.min(...values)) / center) * 100;
}

async function unwrapBetter(task) {
  const outcome = await Task.execute(task);
  if (Result.isError(outcome)) throw new Error("Unexpected better-primitives failure");
  return outcome.value;
}

async function unwrapEffect(effect) {
  const outcome = await Effect.runPromiseExit(effect);
  if (Exit.isFailure(outcome)) throw new Error("Unexpected Effect failure");
  return outcome.value;
}

async function unwrapBetterStream(stream) {
  const outcome = await Stream.runCollect(stream);
  if (Result.isError(outcome)) throw new Error("Unexpected better-primitives stream failure");
  return outcome.value;
}

const increment = (value) => value + 1;
const mapDepth = 10;
const collectionSize = 100;
const streamSize = 1_000;

const betterSucceed = Task.succeed(1);
const effectSucceed = Effect.succeed(1);

let betterMap = Task.succeed(0);
let effectMap = Effect.succeed(0);
for (let index = 0; index < mapDepth; index += 1) {
  betterMap = Task.map(betterMap, increment);
  effectMap = Effect.map(effectMap, increment);
}

let betterFlatMap = Task.succeed(0);
let effectFlatMap = Effect.succeed(0);
for (let index = 0; index < mapDepth; index += 1) {
  betterFlatMap = Task.flatMap(betterFlatMap, (value) => Task.succeed(value + 1));
  effectFlatMap = Effect.flatMap(effectFlatMap, (value) => Effect.succeed(value + 1));
}

const collectionValues = Array.from({ length: collectionSize }, (_, index) => index);
const betterAll = Task.map(
  Task.all(collectionValues.map((value) => Task.succeed(value))),
  (values) => values.at(-1),
);
const effectAll = Effect.map(
  Effect.all(
    collectionValues.map((value) => Effect.succeed(value)),
    {
      concurrency: "unbounded",
    },
  ),
  (values) => values.at(-1),
);

const expectedFailure = { _tag: "ExpectedFailure" };
const betterFailure = Task.fail(expectedFailure);
const effectFailure = Effect.fail(expectedFailure);

const streamValues = Array.from({ length: streamSize }, (_, index) => index);
const betterStream = pipe(
  Stream.from(streamValues),
  Stream.map((value) => value + 1),
);
const effectStream = EffectStream.runCollect(
  EffectStream.map(EffectStream.fromIterable(streamValues), (value) => value + 1),
);

async function nativeMapChain() {
  let operation = Promise.resolve(0);
  for (let index = 0; index < mapDepth; index += 1) operation = operation.then(increment);
  return operation;
}

async function nativeFlatMapChain() {
  let operation = Promise.resolve(0);
  for (let index = 0; index < mapDepth; index += 1) {
    operation = operation.then((value) => Promise.resolve(value + 1));
  }
  return operation;
}

async function nativeAll() {
  const values = await Promise.all(collectionValues.map((value) => Promise.resolve(value)));
  return values.at(-1);
}

async function nativeStreamCollect() {
  async function* source() {
    yield* streamValues;
  }

  const values = [];
  for await (const value of source()) values.push(value + 1);
  return values.at(-1);
}

const scenarios = [
  {
    name: "succeed + typed execution",
    expected: 1,
    implementations: [
      { name: "better-primitives", run: () => unwrapBetter(betterSucceed) },
      { name: "Effect", run: () => unwrapEffect(effectSucceed) },
      { name: "native Promise*", run: () => Promise.resolve(1) },
    ],
  },
  {
    name: `${mapDepth} map steps + typed execution`,
    expected: mapDepth,
    implementations: [
      { name: "better-primitives", run: () => unwrapBetter(betterMap) },
      { name: "Effect", run: () => unwrapEffect(effectMap) },
      { name: "native Promise*", run: nativeMapChain },
    ],
  },
  {
    name: `${mapDepth} flatMap steps + typed execution`,
    expected: mapDepth,
    implementations: [
      { name: "better-primitives", run: () => unwrapBetter(betterFlatMap) },
      { name: "Effect", run: () => unwrapEffect(effectFlatMap) },
      { name: "native Promise*", run: nativeFlatMapChain },
    ],
  },
  {
    name: `all ${collectionSize} successes (unbounded concurrency)`,
    expected: collectionSize - 1,
    implementations: [
      { name: "better-primitives", run: () => unwrapBetter(betterAll) },
      { name: "Effect", run: () => unwrapEffect(effectAll) },
      { name: "native Promise*", run: nativeAll },
    ],
  },
  {
    name: "expected failure + typed execution",
    expected: expectedFailure,
    implementations: [
      {
        name: "better-primitives",
        run: async () => {
          const outcome = await Task.execute(betterFailure);
          if (Result.isOk(outcome)) throw new Error("Expected better-primitives to fail");
          return outcome.error;
        },
      },
      {
        name: "Effect",
        run: async () => {
          const outcome = await Effect.runPromiseExit(effectFailure);
          if (Exit.isSuccess(outcome)) throw new Error("Expected Effect to fail");
          return Option.getOrThrow(Cause.failureOption(outcome.cause));
        },
      },
      {
        name: "native Promise*",
        run: () => Promise.reject(expectedFailure).catch((error) => error),
      },
    ],
  },
  {
    name: `stream map + collect ${streamSize} values`,
    expected: streamSize,
    implementations: [
      {
        name: "better-primitives",
        run: async () => {
          const values = await unwrapBetterStream(betterStream);
          return values.at(-1);
        },
      },
      {
        name: "Effect",
        run: async () => {
          const values = await unwrapEffect(effectStream);
          return Chunk.unsafeGet(values, values.length - 1);
        },
      },
      { name: "native AsyncIterable*", run: nativeStreamCollect },
    ],
  },
];

async function benchmarkScenario(scenario) {
  for (const implementation of scenario.implementations) {
    const actual = await implementation.run();
    if (!Object.is(actual, scenario.expected)) {
      throw new Error(
        `${scenario.name}/${implementation.name}: expected ${String(scenario.expected)}, got ${String(actual)}`,
      );
    }
    await measure(implementation.run, config.warmupMs);
  }

  const rates = new Map(scenario.implementations.map(({ name }) => [name, []]));
  for (let sample = 0; sample < config.samples; sample += 1) {
    for (let offset = 0; offset < scenario.implementations.length; offset += 1) {
      const index = (sample + offset) % scenario.implementations.length;
      const implementation = scenario.implementations[index];
      rates.get(implementation.name).push(await measure(implementation.run, config.sampleMs));
    }
  }

  const rows = scenario.implementations.map(({ name }) => {
    const samples = rates.get(name);
    const opsPerSecond = median(samples);
    return {
      implementation: name,
      opsPerSecond,
      samples,
      spreadPercent: relativeSpread(samples, opsPerSecond),
    };
  });
  const effectRate = rows.find(({ implementation }) => implementation === "Effect").opsPerSecond;
  return {
    name: scenario.name,
    rows: rows.map((row) => ({ ...row, versusEffect: row.opsPerSecond / effectRate })),
  };
}

function formatRate(value) {
  return Math.round(value).toLocaleString("en-US");
}

function printScenario(result) {
  const rows = result.rows.map((row) => [
    row.implementation,
    formatRate(row.opsPerSecond),
    `${row.versusEffect.toFixed(2)}x`,
    `${row.spreadPercent.toFixed(1)}%`,
  ]);
  const headers = ["implementation", "median ops/s", "vs Effect", "sample spread"];
  const widths = headers.map((header, index) =>
    Math.max(header.length, ...rows.map((row) => row[index].length)),
  );
  const formatRow = (row) => row.map((cell, index) => cell.padEnd(widths[index])).join("  ");

  console.log(`\n${result.name}`);
  console.log(formatRow(headers));
  console.log(formatRow(widths.map((width) => "-".repeat(width))));
  for (const row of rows) console.log(formatRow(row));
}

const environment = {
  architecture: process.arch,
  effectVersion,
  nodeVersion: process.version,
  platform: process.platform,
};
const results = [];

for (const scenario of scenarios) {
  if (!json) process.stdout.write(`Benchmarking ${scenario.name}...\r`);
  results.push(await benchmarkScenario(scenario));
}

if (json) {
  console.log(JSON.stringify({ config, environment, results }, null, 2));
} else {
  console.log(" ".repeat(100));
  console.log(
    `Node ${environment.nodeVersion} | Effect ${effectVersion} | ${environment.platform} ${environment.architecture}`,
  );
  console.log(
    `${config.samples} samples × ${config.sampleMs}ms after ${config.warmupMs}ms warmup; higher is better`,
  );
  for (const result of results) printScenario(result);
  console.log(
    "\n* Native rows are lower-bound baselines and do not provide equivalent typed outcomes or runtime semantics.",
  );
}

void blackhole;
