import { file, write } from "bun";
import { defineConfig, type UserConfig } from "tsdown";

type DistPackageJson = {
  [key: string]: unknown;
  main?: string;
  module?: string;
  typings?: string;
  types?: string;
  exports?: Record<string, unknown>;
  unpkg?: string;
  jsdelivr?: string;
  scripts?: unknown;
  devDependencies?: unknown;
  private?: unknown;
  workspaces?: unknown;
};

const packageJsonUrl = new URL("./package.json", import.meta.url);
const distPackageJsonUrl = new URL("./dist/package.json", import.meta.url);
const entry = "src/public_api.ts";

async function writeDistPackageJson() {
  const packageJson = (await file(packageJsonUrl).json()) as DistPackageJson;

  packageJson.main = "./index.js";
  packageJson.module = "./index.js";
  packageJson.typings = "./index.d.ts";
  packageJson.types = "./index.d.ts";
  packageJson.exports = {
    "./package.json": "./package.json",
    ".": {
      types: "./index.d.ts",
      default: "./index.js",
    },
    "./otel": {
      types: "./otel.d.ts",
      default: "./otel.js",
    },
  };
  packageJson.unpkg = "./index.min.js";
  packageJson.jsdelivr = "./index.min.js";
  delete packageJson.scripts;
  delete packageJson.devDependencies;
  delete packageJson.private;
  // The docs and examples workspaces are repository tooling, not part of the published package.
  delete packageJson.workspaces;

  await write(distPackageJsonUrl, `${JSON.stringify(packageJson, null, 2)}\n`);
}

export default defineConfig([
  {
    format: "esm",
    outDir: "dist",
    platform: "neutral",
    target: false,
    tsconfig: "tsconfig.lib.json",
    clean: true,
    dts: true,
    entry: {
      index: entry,
      otel: "src/otel.ts",
    },
    inputOptions: {
      resolve: {
        mainFields: ["module", "main"],
      },
    },
    copy: ["README.md", "LICENSE"],
    hooks: {
      "build:done": async () => {
        await writeDistPackageJson();
      },
    },
  } satisfies UserConfig,
  {
    format: "esm",
    outDir: "dist",
    platform: "neutral",
    target: false,
    tsconfig: "tsconfig.lib.json",
    clean: false,
    dts: false,
    entry: {
      "index.min": entry,
    },
    minify: true,
    inputOptions: {
      resolve: {
        mainFields: ["module", "main"],
      },
    },
  } satisfies UserConfig,
]);
