## ⚠️ Before version 0.1.0, there will be significant API adjustments. If needed, please pin the version number.


# CK-ORM

<p align="center">
  <img src="bunner.png" alt="ck-orm - TypeScript ORM for ClickHouse" width="100%" />
</p>

<p align="center">
  <a href="https://npmjs.com/package/ck-orm"><img src="https://img.shields.io/npm/v/ck-orm?style=flat-square&labelColor=%23151A1F&color=%23151A1F" alt="npm package"></a>
  <a href="https://npmjs.com/package/ck-orm"><img src="https://img.shields.io/npm/dm/ck-orm?style=flat-square&labelColor=%23151A1F&color=%23151A1F" alt="monthly downloads"></a>
  <a href="https://github.com/MunMunMiao/ck-orm/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/MunMunMiao/ck-orm/ci.yml?branch=main&style=flat-square&labelColor=%23151A1F&color=%23151A1F" alt="build status"></a>
  <a href="https://github.com/MunMunMiao/ck-orm/blob/main/LICENSE"><img src="https://img.shields.io/github/license/MunMunMiao/ck-orm?style=flat-square&labelColor=%23151A1F&color=%23151A1F" alt="license"></a>
</p>

<p align="center">
  <a href="https://deepwiki.com/MunMunMiao/ck-orm"><img src="https://img.shields.io/badge/Ask_DeepWiki-151A1F?style=flat-square&logo=data%3Aimage%2Fsvg%2Bxml%3Bbase64%2CPHN2ZyB4bWxucz0naHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmcnIHZpZXdCb3g9JzAgMCAzMiAzMic%2BPGcgZmlsbD0nI0Y4QzAwMCc%2BPGNpcmNsZSBjeD0nNi41JyBjeT0nMTYnIHI9JzUuMjUnLz48Y2lyY2xlIGN4PScxMS41JyBjeT0nOScgcj0nNScgb3BhY2l0eT0nLjg0Jy8%2BPGNpcmNsZSBjeD0nMjAnIGN5PSc3LjUnIHI9JzUnIG9wYWNpdHk9Jy43MicvPjxjaXJjbGUgY3g9JzI1LjUnIGN5PScxMy41JyByPSc1JyBvcGFjaXR5PScuODgnLz48Y2lyY2xlIGN4PScxNC41JyBjeT0nMTYuNScgcj0nNS4yNScvPjxjaXJjbGUgY3g9JzknIGN5PScyNCcgcj0nNScgb3BhY2l0eT0nLjc4Jy8%2BPGNpcmNsZSBjeD0nMTguNScgY3k9JzIzLjUnIHI9JzUnIG9wYWNpdHk9Jy44MicvPjxjaXJjbGUgY3g9JzI0JyBjeT0nMTknIHI9JzUnIG9wYWNpdHk9Jy43NCcvPjwvZz48L3N2Zz4%3D" alt="Ask DeepWiki"></a>
</p>

<p align="center">
  A typed ClickHouse query layer for modern JavaScript runtimes.
</p>

It gives you:

- a schema DSL for ClickHouse tables and columns
- a typed query builder for the common path
- raw SQL when ClickHouse-specific syntax is the better tool
- session helpers for temporary-table workflows
- observability hooks for logging, tracing, and custom instrumentation

## Installation

```bash
bun add ck-orm
```

```bash
npm install ck-orm
```

Yarn does not install peer dependencies, so add `@opentelemetry/api` yourself: `yarn add ck-orm @opentelemetry/api`.

## Example

```ts
import { ck, ckTable, ckType, clickhouseClient, fn } from "ck-orm";

const probes = ckTable("probes", {
  probeId: ckType.string("probe_id"),
  signal: ckType.decimal({ precision: 20, scale: 5 }),
  status: ckType.int16(),
});

const db = clickhouseClient({ host: "http://127.0.0.1:8123" });

const rows = await db
  .select({ probeId: probes.probeId, total: fn.sum(probes.signal).as("total") })
  .from(probes)
  .where(ck.eq(probes.status, 1))
  .groupBy(probes.probeId);
// rows: { probeId: string; total: string }[]
```

## Documentation

- **[ck-orm.lyz.cloud](https://ck-orm.lyz.cloud)**: the full reference, covering the schema DSL, query builder, writes, raw SQL, sessions, observability, errors and security.
- **[llms.txt](https://ck-orm.lyz.cloud/llms.txt)**: the same reference for AI coding agents, with one markdown file per section.
- **[`examples/`](./examples)**: runnable examples, each a `main.ts` with a `main.test.ts` and a `package.json`; the site shows the SQL each one sends.
- **[`e2e/`](./e2e)**: runnable scenarios against a real ClickHouse server.

The documentation source lives in [`docs/`](./docs).

## License

[MIT](./LICENSE)
