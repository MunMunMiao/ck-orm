import { describe, expect, it } from "bun:test";
import { spawnSync } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ckType, isClickHouseORMError } from "./public_api";

const fixtureRoot = join(import.meta.dir, "observability-rejections-fixture.test.ts");
const runtimes = ["node", "bun"] as const;
const loggerScenarios = ["start", "success", "error"] as const;
const validatorPaths = {
  unit: ["encode", "decode"],
  e2e: ["insert-values", "insert-json", "select", "iterator"],
} as const;

const runCase = async (input: {
  id: "REL-003" | "REL-004";
  layer: "unit" | "e2e";
  runtime: (typeof runtimes)[number];
  scenario: string;
  behavior: string;
}) => {
  const dir = await mkdtemp(join(tmpdir(), "ck-orm-obs-"));
  const outfile = join(dir, "obs-fixture.mjs");
  let requests = 0;
  const server =
    input.layer === "e2e"
      ? Bun.serve({
          hostname: "127.0.0.1",
          port: 0,
          async fetch(request) {
            requests += 1;
            const body = await request.text();
            if (input.id === "REL-003") {
              return input.scenario === "error"
                ? new Response("audit operation error", { status: 500 })
                : Response.json({ data: [{ one: 7 }] });
            }
            if (!/\bselect\b/i.test(body)) {
              return new Response("");
            }
            return input.scenario === "iterator"
              ? new Response('{"value":"safe"}\n')
              : Response.json({ data: [{ value: "safe" }] });
          },
        })
      : undefined;
  try {
    const build = spawnSync(
      process.execPath,
      ["--no-env-file", "--no-install", "build", fixtureRoot, "--outfile", outfile, "--target=node"],
      {
        encoding: "utf8",
        timeout: 8_000,
        env: { PATH: process.env.PATH ?? "", NO_COLOR: "1" },
      },
    );
    expect(build.status, `bundle ${input.runtime} ${input.scenario}`).toBe(0);
    const executable = input.runtime === "bun" ? process.execPath : "node";
    const argv =
      input.runtime === "bun"
        ? [
            "--no-env-file",
            "--no-install",
            fixtureRoot,
            "--ck-orm-fixture",
            input.id,
            input.layer,
            input.scenario,
            input.behavior,
          ]
        : [outfile, "--ck-orm-fixture", input.id, input.layer, input.scenario, input.behavior];
    if (server) {
      argv.push(server.url.href);
    }
    const child = Bun.spawn([executable, ...argv], {
      stdout: "pipe",
      stderr: "pipe",
      stdin: "ignore",
      env: { PATH: process.env.PATH ?? "", NO_COLOR: "1" },
    });
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGKILL");
    }, 8_000);
    const [status, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);
    clearTimeout(timer);
    const marker = `AUDIT_${input.id}_${input.scenario}_INDEPENDENT_REJECTION`;
    expect(timedOut, `${input.runtime} ${input.scenario} ${input.behavior} timeout`).toBe(false);
    expect(stdout, `${input.runtime} ${input.scenario} ${input.behavior}`).toContain("AUDIT_READY");
    expect(stderr).not.toContain("AUDIT_HARNESS_FAILURE");
    expect(stdout).toContain("AUDIT_TARGET_CALLED");
    if (status !== 0) {
      expect(stderr).toContain(marker);
    }
    if (input.id === "REL-004" && input.behavior !== "sync-valid") {
      expect(stdout).toContain("AUDIT_ORM_CAUGHT");
    }
    if (input.id === "REL-003" && input.layer === "e2e" && input.scenario === "error") {
      expect(stdout).toContain("AUDIT_ORM_CAUGHT");
    }
    expect(status).toBe(0);
    expect(stdout).toContain("AUDIT_RESULT");
    expect(stdout).toContain("AUDIT_DRAINED");
    if (input.id === "REL-003" && input.behavior !== "resolve") {
      const hooks: Record<string, string> = { start: "onQueryStart", success: "onQuerySuccess", error: "onQueryError" };
      expect(stderr).toContain(`[ck-orm] plugin "logger" ${hooks[input.scenario]} threw:`);
      expect(stderr).toContain(marker);
    } else {
      expect(stderr).not.toContain(marker);
    }
    if (input.layer === "e2e") {
      const rejectedInsert =
        input.id === "REL-004" && input.scenario.startsWith("insert-") && input.behavior !== "sync-valid";
      expect(requests).toBe(rejectedInsert ? 0 : 1);
    }
  } finally {
    await server?.stop(true);
    await rm(dir, { recursive: true, force: true });
  }
};

describe("REL-003 logger rejection isolation", () => {
  for (const layer of ["unit", "e2e"] as const) {
    for (const runtime of runtimes) {
      for (const scenario of loggerScenarios) {
        it(`REL-003 RED: ${layer} ${runtime} ${scenario} async logger rejection stays inside plugin isolation`, async () => {
          await runCase({ id: "REL-003", layer, runtime, scenario, behavior: "reject" });
        }, 15_000);
        it(`REL-003 CONTROL: ${layer} ${runtime} ${scenario} resolved async logger preserves the operation`, async () => {
          await runCase({ id: "REL-003", layer, runtime, scenario, behavior: "resolve" });
        }, 15_000);
        it(`REL-003 CONTROL: ${layer} ${runtime} ${scenario} synchronous logger throw is already isolated`, async () => {
          await runCase({ id: "REL-003", layer, runtime, scenario, behavior: "sync-throw" });
        }, 15_000);
      }
    }
  }
});

describe("REL-004 validator rejection observation", () => {
  for (const layer of ["unit", "e2e"] as const) {
    for (const runtime of runtimes) {
      for (const scenario of validatorPaths[layer]) {
        it(`REL-004 RED: ${layer} ${runtime} ${scenario} rejecting async validator leaves only the caught ORM error`, async () => {
          await runCase({ id: "REL-004", layer, runtime, scenario, behavior: "reject" });
        }, 15_000);
        it(`REL-004 CONTROL: ${layer} ${runtime} ${scenario} fulfilled async validator is still explicitly unsupported`, async () => {
          await runCase({ id: "REL-004", layer, runtime, scenario, behavior: "resolve" });
        }, 15_000);
        it(`REL-004 CONTROL: ${layer} ${runtime} ${scenario} synchronous invalid value has one catchable error`, async () => {
          await runCase({ id: "REL-004", layer, runtime, scenario, behavior: "sync-invalid" });
        }, 15_000);
        it(`REL-004 CONTROL: ${layer} ${runtime} ${scenario} synchronous valid value preserves normal operation`, async () => {
          await runCase({ id: "REL-004", layer, runtime, scenario, behavior: "sync-valid" });
        }, 15_000);
      }
    }
  }

  it("REL-004 in-process: rejecting async validator is observed at mapToDriverValue and mapFromDriverValue", async () => {
    let validateCalls = 0;
    const column = ckType.string().$validator({
      "~standard": {
        version: 1,
        vendor: "audit-coverage-inprocess",
        validate() {
          validateCalls += 1;
          return Promise.reject(new Error("AUDIT_REL004_INPROCESS_REJECTION"));
        },
      },
    });
    expect(validateCalls).toBe(0);
    try {
      column.mapToDriverValue("audit value");
      throw new Error("encode should throw");
    } catch (error) {
      expect(isClickHouseORMError(error)).toBe(true);
      expect(error).toEqual(
        expect.objectContaining({
          kind: "client_validation",
        }),
      );
      expect(String(error)).toContain("Async Standard Schema validators are not supported on the insert path");
    }
    try {
      column.mapFromDriverValue("audit value");
      throw new Error("decode should throw");
    } catch (error) {
      expect(isClickHouseORMError(error)).toBe(true);
      expect(error).toEqual(
        expect.objectContaining({
          kind: "decode",
        }),
      );
      expect(String(error)).toContain("Async Standard Schema validators are not supported on the decode path");
    }
    expect(validateCalls).toBe(2);
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
  });
});
