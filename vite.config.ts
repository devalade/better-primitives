import { defineConfig } from "vite-plus";

export default defineConfig({
  test: {
    include: ["src/**/*.test.ts"],
  },
  lint: {
    ignorePatterns: ["dist/**", "node_modules/**"],
  },
  fmt: {
    semi: true,
    singleQuote: false,
  },
  pack: {
    entry: [
      "src/index.ts",
      "src/cancel/index.ts",
      "src/time/index.ts",
      "src/scope/index.ts",
      "src/task/index.ts",
      "src/resource/index.ts",
      "src/sync/index.ts",
      "src/queue/index.ts",
      "src/stream/index.ts",
      "src/errors/index.ts",
    ],
    format: ["esm"],
    dts: true,
    sourcemap: true,
    clean: true,
    treeshake: true,
    target: "es2022",
    deps: {
      neverBundle: ["better-result"],
    },
  },
});
