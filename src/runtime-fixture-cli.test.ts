import { describe, expect, it } from "bun:test";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const restrictedEnv = { PATH: process.env.PATH ?? "", NO_COLOR: "1" };
const fixtures = {
  gc: join(import.meta.dir, "runtime-session-gc-fixture.test.ts"),
  plugin: join(import.meta.dir, "runtime-plugin-performance-fixture.test.ts"),
  dsn: join(import.meta.dir, "runtime-boundaries-dsn-fixture.test.ts"),
  observability: join(import.meta.dir, "observability-rejections-fixture.test.ts"),
} as const;
const sentinel = "--ck-orm-fixture";

const runBun = (fixture: string, args: string[]) =>
  spawnSync(process.execPath, ["--no-env-file", "--no-install", fixture, ...args], {
    encoding: "utf8",
    timeout: 8_000,
    env: restrictedEnv,
  });

const runNode = (outfile: string, args: string[], extra: string[] = []) =>
  spawnSync("node", [...extra, outfile, ...args], {
    encoding: "utf8",
    timeout: 8_000,
    env: restrictedEnv,
  });

const buildNode = (fixture: string, outfile: string) =>
  spawnSync(
    process.execPath,
    ["--no-env-file", "--no-install", "build", fixture, "--outfile", outfile, "--target=node"],
    { encoding: "utf8", timeout: 8_000, env: restrictedEnv },
  );

const expectDriverFailure = (result: ReturnType<typeof spawnSync>, label: string) => {
  expect(result.error, `${label} spawn`).toBeUndefined();
  expect(result.status, `${label} must exit nonzero for a driver invocation with a malformed mode`).not.toBe(0);
  expect(result.stderr, `${label} must report a diagnostic`).not.toBe("");
};

const expectNotDriver = (result: ReturnType<typeof spawnSync>, label: string) => {
  expect(result.error, `${label} spawn`).toBeUndefined();
  expect(result.status, `${label} without ${sentinel} is not a driver invocation`).toBe(0);
  expect(result.stderr).not.toContain("AUDIT_HARNESS_FAILURE");
  expect(result.stderr).not.toContain("Unknown DSN fixture scenario");
  expect(result.stderr).not.toContain("Explicit GC is required");
  expect(result.stdout).not.toContain('"id":"PERF-006"');
  expect(result.stdout).not.toContain("AUDIT_READY");
  expect(result.stdout).not.toContain('"errorCaught":true');
};

describe("fixture CLI driver ownership", () => {
  it("RED: explicit --ck-orm-fixture with missing or invalid mode must not silently succeed (Bun)", () => {
    expectDriverFailure(runBun(fixtures.gc, [sentinel]), "gc missing mode");
    expectDriverFailure(runBun(fixtures.gc, [sentinel, "database"]), "gc invalid mode");
    expectDriverFailure(runBun(fixtures.dsn, [sentinel]), "dsn missing scenario");
    expectDriverFailure(runBun(fixtures.dsn, [sentinel, "not-a-scenario"]), "dsn invalid scenario");
    expectDriverFailure(runBun(fixtures.observability, [sentinel]), "observability missing id");
    expectDriverFailure(
      runBun(fixtures.observability, [sentinel, "REL-999", "unit", "start", "resolve"]),
      "observability invalid id",
    );
    expectDriverFailure(runBun(fixtures.plugin, [sentinel]), "plugin missing mode");
    expectDriverFailure(runBun(fixtures.plugin, [sentinel, "--other"]), "plugin invalid mode");
    expectDriverFailure(runBun(fixtures.plugin, [sentinel, "--observed", "--unobserved"]), "plugin conflicting flags");
  });

  it("RED: explicit --ck-orm-fixture with missing or invalid mode must not silently succeed (Node)", async () => {
    const dir = await mkdtemp(join(tmpdir(), "ck-orm-fixture-cli-"));
    try {
      const outfiles = {
        gc: join(dir, "gc.mjs"),
        dsn: join(dir, "dsn.mjs"),
        observability: join(dir, "obs.mjs"),
        plugin: join(dir, "plugin.mjs"),
      };
      for (const [name, fixture] of [
        ["gc", fixtures.gc],
        ["dsn", fixtures.dsn],
        ["observability", fixtures.observability],
        ["plugin", fixtures.plugin],
      ] as const) {
        const build = buildNode(fixture, outfiles[name]);
        expect(build.status, `${name} bundle ${build.stderr}`).toBe(0);
      }
      expectDriverFailure(runNode(outfiles.gc, [sentinel, "database"]), "node gc invalid mode");
      expectDriverFailure(runNode(outfiles.dsn, [sentinel, "not-a-scenario"]), "node dsn invalid scenario");
      expectDriverFailure(
        runNode(outfiles.observability, [sentinel, "REL-999", "unit", "start", "resolve"]),
        "node observability invalid id",
      );
      expectDriverFailure(
        runNode(outfiles.plugin, [sentinel, "--observed", "--unobserved"]),
        "node plugin conflicting flags",
      );
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }, 30_000);

  it("CONTROL: direct invocation without --ck-orm-fixture is not a driver invocation even when leftover args look like a mode", () => {
    expectNotDriver(runBun(fixtures.gc, ["unit"]), "gc leftover unit");
    expectNotDriver(runBun(fixtures.dsn, ["invalid-port"]), "dsn leftover invalid-port");
    expectNotDriver(runBun(fixtures.plugin, ["--observed"]), "plugin leftover --observed");
    expectNotDriver(
      runBun(fixtures.observability, ["REL-003", "unit", "start", "resolve"]),
      "observability leftover REL-003",
    );
    expectNotDriver(runBun(fixtures.gc, []), "gc discovery-equivalent");
    expectNotDriver(runBun(fixtures.dsn, []), "dsn discovery-equivalent");
    expectNotDriver(runBun(fixtures.plugin, []), "plugin discovery-equivalent");
    expectNotDriver(runBun(fixtures.observability, []), "observability discovery-equivalent");
  });

  it("RED: SEC-006 nested Bun build must use process.execPath, --no-env-file --no-install, restricted env, and a timeout", () => {
    const source = readFileSync(join(import.meta.dir, "runtime-boundaries.test.ts"), "utf8");
    const block = source.slice(source.indexOf("SEC-006 RED: Bun and Node subprocess serialization stay safe"));
    expect(block.length).toBeGreaterThan(0);
    expect(block).toContain("process.execPath");
    expect(block).toMatch(/"--no-env-file",\s*"--no-install",\s*"build"/);
    expect(block).toMatch(/timeout:\s*8_000/);
    expect(block).toContain('NO_COLOR: "1"');
    expect(block).not.toContain('spawnSync("bun", ["build"');
  });
});
