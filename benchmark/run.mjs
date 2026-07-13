import { spawnSync } from "node:child_process";
import process from "node:process";

const json = process.argv.includes("--json");
const build = spawnSync("vp", ["pack"], {
  stdio: json ? ["ignore", "ignore", "inherit"] : "inherit",
  shell: process.platform === "win32",
});

if (build.error) throw build.error;
if (build.status !== 0) process.exit(build.status ?? 1);

await import("./compare.mjs");
