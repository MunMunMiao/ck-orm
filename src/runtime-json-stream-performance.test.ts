import { describe, expect, it } from "bun:test";
import { isClickHouseORMError } from "./errors";
import { clickhouseClient } from "./runtime";
import { createLineStream, parseJsonEachRowLine } from "./runtime/json-stream";
import { sql } from "./sql";

const encoder = new TextEncoder();
const CHUNK_BYTES = 4 * 1024;
const MARKER_SQL = sql`SELECT 1 AS id`;

type SearchWork = {
  calls: number;
  searchedCodeUnits: number;
  receiverCodeUnits: number;
  largestReceiver: number;
};

const longRow = (inputBytes: number) => {
  const value = "x".repeat(inputBytes - 13);
  return { bytes: encoder.encode(`{"value":"${value}"}\n`), expected: [{ value }] };
};

const chunkedResponse = (bytes: Uint8Array, chunkBytes = CHUNK_BYTES) => {
  const state = { pulls: 0, cancelled: 0, bytesRead: 0 };
  const body = new ReadableStream<Uint8Array>(
    {
      pull(controller) {
        if (state.bytesRead === bytes.length) {
          controller.close();
          return;
        }
        const end = Math.min(bytes.length, state.bytesRead + chunkBytes);
        controller.enqueue(bytes.subarray(state.bytesRead, end));
        state.bytesRead = end;
        state.pulls += 1;
      },
      cancel() {
        state.cancelled += 1;
      },
    },
    { highWaterMark: 0 },
  );
  return { response: new Response(body), state };
};

const fragmentedResponse = (chunks: Uint8Array[], rejectCancel: boolean | Error = false) => {
  const state = { pulls: 0, cancelled: 0 };
  let offset = 0;
  const response = new Response(
    new ReadableStream<Uint8Array>(
      {
        pull(controller) {
          state.pulls += 1;
          if (offset === chunks.length) {
            controller.close();
          } else {
            const chunk = chunks[offset];
            offset += 1;
            if (chunk === undefined) {
              throw new Error("missing fragmented chunk");
            }
            controller.enqueue(chunk);
          }
        },
        cancel() {
          state.cancelled += 1;
          if (rejectCancel) {
            throw rejectCancel instanceof Error ? rejectCancel : new Error("audit_perf_001_cancel_failure");
          }
        },
      },
      { highWaterMark: 0 },
    ),
  );
  return { response, state };
};

const inMemoryClient = (response: () => Response) =>
  clickhouseClient({
    host: "http://127.0.0.1:1",
    database: "ck_orm_audit_memory_only",
    fetch: () => response(),
  });

const collect = async <TValue>(iterable: AsyncIterable<TValue>) => {
  const rows: TValue[] = [];
  for await (const row of iterable) {
    rows.push(row);
  }
  return rows;
};

const parseResponse = async (response: Response) => {
  const rows: unknown[] = [];
  for await (const line of createLineStream(response)) {
    rows.push(parseJsonEachRowLine({ line, response, queryId: "audit_perf_001_unit", json: JSON }));
  }
  return rows;
};

const parseThroughPublicAPI = (bytes: Uint8Array, chunkBytes: number) => {
  const db = inMemoryClient(() => chunkedResponse(bytes, chunkBytes).response);
  return collect(db.stream(sql`SELECT 'fixture' AS value`));
};

const rowsFrom = (chunks: Uint8Array[]) => {
  const db = inMemoryClient(() => fragmentedResponse(chunks).response);
  return collect(db.stream(sql`SELECT 'adversarial' AS value`));
};

const measureSearchWork = async <TValue>(run: () => Promise<TValue>) => {
  const original = String.prototype.indexOf;
  const work: SearchWork = { calls: 0, searchedCodeUnits: 0, receiverCodeUnits: 0, largestReceiver: 0 };
  String.prototype.indexOf = function (this: string, needle: string, position?: number) {
    const result = original.call(this, needle, position);
    if (needle === "\n") {
      const length = this.length;
      const start = Math.min(length, Math.max(0, Math.trunc(position || 0)));
      work.calls += 1;
      work.searchedCodeUnits += (result < 0 ? length : result + 1) - start;
      work.receiverCodeUnits += length;
      work.largestReceiver = Math.max(work.largestReceiver, length);
    }
    return result;
  };
  try {
    return { value: await run(), work };
  } finally {
    String.prototype.indexOf = original;
  }
};

const assertLinearSearch = (work: SearchWork, inputCodeUnits: number) => {
  expect(work.calls).toBeGreaterThan(0);
  expect(work.searchedCodeUnits).toBeLessThanOrEqual(inputCodeUnits * 2);
};

describe("PERF-001 JSONEachRow line-search budget", () => {
  it("PERF-001 RED: a cross-chunk long row searches only linear input ranges", async () => {
    const { bytes, expected } = longRow(256 * 1024);
    const result = await measureSearchWork(() => parseResponse(chunkedResponse(bytes).response));
    expect(result.value).toEqual(expected);
    assertLinearSearch(result.work, bytes.length);
  });

  it("PERF-001 RED: an unterminated long tail does not rescan accumulated prefixes", async () => {
    const { bytes, expected } = longRow(256 * 1024);
    const tail = bytes.subarray(0, -1);
    const result = await measureSearchWork(() => parseResponse(chunkedResponse(tail).response));
    expect(result.value).toEqual(expected);
    assertLinearSearch(result.work, tail.length);
  });

  it("PERF-001 RED: several unequal long rows use linear search after completed prefixes", async () => {
    const expected = [
      { value: "prefix" },
      { value: "a".repeat(24 * 1024) },
      { value: "middle" },
      { value: "b".repeat(48 * 1024) },
      { value: "tail" },
    ];
    const text = expected.map((row) => JSON.stringify(row)).join("\n");
    const bytes = encoder.encode(text);
    const chunks: Uint8Array[] = [];
    const sizes = [1024, 511, 2048, 97];
    for (let offset = 0, i = 0; offset < bytes.length; i += 1) {
      const size = sizes[i % sizes.length];
      if (size === undefined) {
        throw new Error("missing chunk size");
      }
      const end = Math.min(bytes.length, offset + size);
      chunks.push(bytes.subarray(offset, end));
      offset = end;
    }
    const result = await measureSearchWork(() => rowsFrom(chunks));
    expect(result.value).toEqual(expected);
    expect(result.work.calls).toBeGreaterThan(0);
    expect(result.work.searchedCodeUnits).toBeLessThanOrEqual(text.length * 2);
  });

  it("PERF-001 CONTROL: one-chunk long row has linear search and exact JSON", async () => {
    const { bytes, expected } = longRow(256 * 1024);
    const result = await measureSearchWork(() => parseResponse(chunkedResponse(bytes, bytes.length).response));
    expect(result.value).toEqual(expected);
    assertLinearSearch(result.work, bytes.length);
  });

  it("PERF-001 CONTROL: UTF-8 split at every byte preserves escaped text and tail", async () => {
    const expected = [{ value: '猫🙂é\n"\\' }, { value: "尾行" }];
    const input = encoder.encode(` \r\n${JSON.stringify(expected[0])}\r\n\t\n${JSON.stringify(expected[1])}`);
    for (const chunkBytes of [1, 2, 3, 4, 7, input.length]) {
      expect(await parseThroughPublicAPI(input, chunkBytes)).toEqual(expected);
    }
  });

  it("PERF-001 CONTROL: mixed short and long rows survive compaction and empty lines", async () => {
    const shortRows = Array.from({ length: 8_000 }, (_, id) => ({ id }));
    const long = { value: "x".repeat(96 * 1024) };
    const expected = [...shortRows, long, { value: "tail" }];
    const input = encoder.encode(
      `${shortRows.map((row) => JSON.stringify(row)).join("\n")}\n \r\n${JSON.stringify(long)}\n\n{"value":"tail"}`,
    );
    expect(await parseThroughPublicAPI(input, CHUNK_BYTES)).toEqual(expected);
  });

  it("PERF-001 CONTROL: empty and whitespace-only bodies produce no rows", async () => {
    for (const text of ["", "\r\n \n\t", " \n\n "]) {
      expect(await parseThroughPublicAPI(encoder.encode(text), 1)).toEqual([]);
    }
    expect(await parseResponse(new Response(null))).toEqual([]);
  });

  it("PERF-001 CONTROL: a newline at either side of a chunk boundary loses no row", async () => {
    const input = encoder.encode('{"id":1}\n{"id":2}\n{"id":3}');
    for (const chunkBytes of [8, 9, 10, 17, 18, 19]) {
      expect(await parseThroughPublicAPI(input, chunkBytes)).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
    }
  });

  it("PERF-001 CONTROL: early consumer return cancels before pulling the long remainder", async () => {
    const fixture = chunkedResponse(encoder.encode(`{"id":1}\n{"value":"${"x".repeat(256 * 1024)}"}\n`), 9);
    const db = inMemoryClient(() => fixture.response);
    const iterator = db.stream(MARKER_SQL);
    try {
      expect(await iterator.next()).toEqual({ value: { id: 1 }, done: false });
      await iterator.return();
      expect(fixture.state.cancelled).toBe(1);
      expect(fixture.state.bytesRead).toBe(9);
      expect(fixture.state.pulls).toBe(1);
    } finally {
      await iterator.return();
    }
  });

  it("PERF-001 CONTROL: embedded server exception after a row remains an error", async () => {
    const input = encoder.encode('{"id":1}\nCode: 60. DB::Exception: audit_perf_001_missing. (UNKNOWN_TABLE)\n');
    const fixture = chunkedResponse(input, 5);
    const db = inMemoryClient(() => fixture.response);
    const iterator = db.stream(MARKER_SQL, { query_id: "audit_perf_001_error" });
    try {
      expect(await iterator.next()).toEqual({ value: { id: 1 }, done: false });
      let caught: unknown;
      try {
        await iterator.next();
      } catch (error) {
        caught = error;
      }
      expect(isClickHouseORMError(caught)).toBe(true);
      if (!isClickHouseORMError(caught)) {
        throw new Error("missing request_failed");
      }
      expect(caught.kind).toBe("request_failed");
      expect(caught.clickhouseCode).toBe(60);
      expect(caught.queryId).toBe("audit_perf_001_error");
      expect(fixture.state.cancelled).toBe(1);
    } finally {
      await iterator.return();
    }
  });

  it("PERF-001 CONTROL: malformed JSON rejects and cancels instead of returning partial rows", async () => {
    const fixture = chunkedResponse(encoder.encode('{"value":bad}\n{"id":2}\n'), 4);
    const db = inMemoryClient(() => fixture.response);
    const iterator = db.stream(sql`SELECT 1`);
    try {
      let caught: unknown;
      try {
        await collect(iterator);
      } catch (error) {
        caught = error;
      }
      expect(isClickHouseORMError(caught)).toBe(true);
      if (!isClickHouseORMError(caught)) {
        throw new Error("missing request_failed");
      }
      expect(caught.kind).toBe("request_failed");
      expect(fixture.state.cancelled).toBe(1);
    } finally {
      await iterator.return();
    }
  });

  it("PERF-001 CONTROL: read failure during an incomplete line is propagated", async () => {
    const reason = new TypeError("audit_perf_001_read_failure");
    let pulls = 0;
    const response = new Response(
      new ReadableStream<Uint8Array>(
        {
          pull(controller) {
            if (pulls === 0) {
              pulls += 1;
              controller.enqueue(encoder.encode('{"value":"partial'));
            } else {
              controller.error(reason);
            }
          },
        },
        { highWaterMark: 0 },
      ),
    );
    const db = inMemoryClient(() => response);
    const iterator = db.stream(sql`SELECT 1`);
    try {
      let caught: unknown;
      try {
        await collect(iterator);
      } catch (error) {
        caught = error;
      }
      expect(isClickHouseORMError(caught)).toBe(true);
      if (!isClickHouseORMError(caught)) {
        throw new Error("missing transport error");
      }
      expect(caught.cause).toBe(reason);
    } finally {
      await iterator.return();
    }
  });

  it("PERF-001 CONTROL: every byte boundary plus empty chunks preserves BOM, UTF-8 and CRLF", async () => {
    const bom = String.fromCodePoint(0xfeff);
    const expected = [{ value: `猫é𝄞${bom}${String.fromCodePoint(0x200b)}\n\\"` }, { value: "second" }];
    const bytes = encoder.encode(
      `${bom} \r\n\t${JSON.stringify(expected[0])}\r\n${String.fromCodePoint(0xa0)}\n${JSON.stringify(expected[1])}  `,
    );
    for (let split = 0; split <= bytes.length; split += 1) {
      const chunks = [bytes.subarray(0, split), new Uint8Array(), bytes.subarray(split)];
      expect(await rowsFrom(chunks)).toEqual(expected);
    }
    expect(await rowsFrom(Array.from(bytes, (byte) => Uint8Array.of(byte)))).toEqual(expected);
  });

  it("PERF-001 CONTROL: interleaved readers own independent decoder and partial-line state", async () => {
    const left = [
      { value: `猫${String.fromCodePoint(0xfeff)}é`, side: "left" },
      { value: "𠀀", side: "left" },
    ];
    const right = [
      { value: "𝄞Ж", side: "right" },
      { value: "é", side: "right" },
    ];
    const chunksFor = (rows: object[]) =>
      Array.from(encoder.encode(rows.map((row) => JSON.stringify(row)).join("\n")), (byte) => Uint8Array.of(byte));
    const [actualLeft, actualRight] = await Promise.all([rowsFrom(chunksFor(left)), rowsFrom(chunksFor(right))]);
    expect(actualLeft).toEqual(left);
    expect(actualRight).toEqual(right);
  });

  it("PERF-001 CONTROL: early return ignores cancellation failure without consuming the remainder", async () => {
    const fixture = fragmentedResponse(
      [encoder.encode('{"id":1}\n{"id":2}\n{"value":"partial'), encoder.encode(' remainder"}\n')],
      true,
    );
    const db = inMemoryClient(() => fixture.response);
    const iterator = db.stream(MARKER_SQL);
    try {
      expect(await iterator.next()).toEqual({ value: { id: 1 }, done: false });
      expect(fixture.state.pulls).toBe(1);
      expect(await iterator.return()).toEqual({ value: undefined, done: true });
      expect(fixture.state.cancelled).toBe(1);
      expect(fixture.state.pulls).toBe(1);
    } finally {
      await iterator.return();
    }
  });

  it("PERF-001 supplementary CONTROL: final review exhausts three-way UTF-8 splits with empty chunks and no final newline", async () => {
    const bom = String.fromCodePoint(0xfeff);
    const expected = [{ value: `猫é𝄞${bom}\n\\"` }, { value: "tail" }];
    const bytes = encoder.encode(`${bom}\r\n${JSON.stringify(expected[0])}\r\n \n${JSON.stringify(expected[1])}`);
    for (let first = 0; first <= bytes.length; first++) {
      for (let second = first; second <= bytes.length; second++) {
        expect(
          await rowsFrom([
            new Uint8Array(),
            bytes.subarray(0, first),
            new Uint8Array(),
            bytes.subarray(first, second),
            new Uint8Array(),
            bytes.subarray(second),
            new Uint8Array(),
          ]),
          `byte boundaries ${first},${second}`,
        ).toEqual(expected);
      }
    }
  }, 60_000);

  it("PERF-001 supplementary CONTROL: final review preserves pending UTF-8 after exact compaction thresholds", async () => {
    for (const boundary of [65_535, 65_536, 65_537]) {
      const prefix = longRow(boundary);
      const rest = [{ value: "猫".repeat(4096) }, { id: 2 }, { value: "z".repeat(24 * 1024) }, { value: "tail" }];
      const bytes = encoder.encode(
        `${new TextDecoder().decode(prefix.bytes)}${rest.map((row) => JSON.stringify(row)).join("\n")}`,
      );
      const cut = boundary + '{"value":"'.length + 1;
      const chunks = [bytes.subarray(0, cut), new Uint8Array(), bytes.subarray(cut, cut + 1)];
      for (let offset = cut + 1; offset < bytes.length; offset += 1023) {
        chunks.push(bytes.subarray(offset, offset + 1023));
      }
      expect(await rowsFrom(chunks), `prefix bytes=${boundary}`).toEqual([...prefix.expected, ...rest]);
    }
  }, 30_000);

  it("PERF-001 supplementary CONTROL: final review returns after a fragmented row without pulling any unread chunk", async () => {
    const row = { value: "x".repeat(16 * 1024) };
    const bytes = encoder.encode(`${JSON.stringify(row)}\n{"id":2}\n{"value":"pending`);
    const fixture = fragmentedResponse(
      [bytes.subarray(0, 9), new Uint8Array(), bytes.subarray(9), encoder.encode(' remainder"}\n')],
      new Error("audit_perf001_final_cancel_failure"),
    );
    const iterator = inMemoryClient(() => fixture.response).stream(sql`SELECT 1`);
    try {
      expect(await iterator.next()).toEqual({ value: row, done: false });
      expect(fixture.state.pulls).toBe(3);
      expect(await iterator.return()).toEqual({ value: undefined, done: true });
      expect(await iterator.next()).toEqual({ value: undefined, done: true });
      expect(fixture.state).toEqual({ pulls: 3, cancelled: 1 });
    } finally {
      await iterator.return();
    }
  });

  it("PERF-001 supplementary CONTROL: final review cancellation failure cannot mask JSON or server errors after a long row", async () => {
    for (const badLine of ['{"value":NOT_JSON}', "Code: 60. DB::Exception: audit_perf001_final. (UNKNOWN_TABLE)"]) {
      const prefix = longRow(16 * 1024);
      const errorBytes = encoder.encode(`${badLine}\n{"unread":true}\n`);
      const fixture = fragmentedResponse(
        [
          prefix.bytes.subarray(0, 31),
          prefix.bytes.subarray(31),
          errorBytes.subarray(0, 10),
          new Uint8Array(),
          errorBytes.subarray(10),
          encoder.encode('{"never_pulled":true}\n'),
        ],
        new Error("audit_perf001_final_cancel_failure"),
      );
      const iterator = inMemoryClient(() => fixture.response).stream(sql`SELECT 1`, {
        query_id: "audit_perf001_final_error",
      });
      try {
        const firstExpected = prefix.expected[0];
        expect(firstExpected).toBeDefined();
        expect(await iterator.next()).toEqual({ value: firstExpected, done: false });
        let caught: unknown;
        try {
          await iterator.next();
        } catch (error) {
          caught = error;
        }
        expect(isClickHouseORMError(caught)).toBe(true);
        if (!isClickHouseORMError(caught)) {
          throw new Error("missing request_failed");
        }
        expect(caught.kind).toBe("request_failed");
        expect(caught.queryId).toBe("audit_perf001_final_error");
        if (badLine.startsWith("Code:")) {
          expect(caught.clickhouseCode).toBe(60);
        } else {
          expect(caught.cause).toBeInstanceOf(SyntaxError);
        }
        expect(fixture.state).toEqual({ pulls: 5, cancelled: 1 });
      } finally {
        await iterator.return();
      }
    }
  });

  it("PERF-001 supplementary CONTROL: final review aborts a pending read with partial line and UTF-8 state", async () => {
    const external = new AbortController();
    const pendingRead = Promise.withResolvers<void>();
    const reason = new Error("audit_perf001_final_abort");
    let pulls = 0;
    let detach = () => undefined;
    const db = clickhouseClient({
      host: "http://127.0.0.1:1",
      database: "ck_orm_audit_memory_only",
      request_timeout: 5000,
      fetch(request) {
        return new Response(
          new ReadableStream<Uint8Array>(
            {
              start(controller) {
                const onAbort = () => controller.error(request.signal.reason);
                request.signal.addEventListener("abort", onAbort, { once: true });
                detach = () => {
                  request.signal.removeEventListener("abort", onAbort);
                };
              },
              pull(controller) {
                pulls += 1;
                if (pulls === 1) {
                  const text = encoder.encode(`{"id":1}\n{"value":"${"x".repeat(16 * 1024)}`);
                  const bytes = new Uint8Array(text.length + 1);
                  bytes.set(text);
                  bytes[text.length] = 0xe7;
                  controller.enqueue(bytes);
                } else {
                  pendingRead.resolve();
                }
              },
            },
            { highWaterMark: 0 },
          ),
        );
      },
    });
    const iterator = db.stream(sql`SELECT 1`, {
      abort_signal: external.signal,
      query_id: "audit_perf001_final_abort",
    });
    try {
      expect(await iterator.next()).toEqual({ value: { id: 1 }, done: false });
      const next = iterator.next();
      const rejected = next.then(
        () => {
          throw new Error("pending read must reject");
        },
        (error: unknown) => {
          expect(isClickHouseORMError(error)).toBe(true);
          if (!isClickHouseORMError(error)) {
            throw new Error("missing aborted");
          }
          expect(error.kind).toBe("aborted");
          expect(error.cause).toBe(reason);
          expect(error.queryId).toBe("audit_perf001_final_abort");
        },
      );
      await pendingRead.promise;
      external.abort(reason);
      await rejected;
      expect(pulls).toBe(2);
      expect(await iterator.next()).toEqual({ value: undefined, done: true });
    } finally {
      external.abort(reason);
      await iterator.return();
      detach();
    }
  });

  it("PERF-001 supplementary: final review keeps 1 KiB-chunk search linear across three small row sizes", async () => {
    const measured: Array<SearchWork & { inputCodeUnits: number }> = [];
    const nativeIndexOf = String.prototype.indexOf;
    for (const size of [16 * 1024, 32 * 1024, 64 * 1024]) {
      const { bytes, expected } = longRow(size);
      const result = await measureSearchWork(() => collect(createLineStream(chunkedResponse(bytes, 1024).response)));
      expect(result.value.map((line) => JSON.parse(line))).toEqual(expected);
      expect(String.prototype.indexOf).toBe(nativeIndexOf);
      expect(result.work.calls).toBeGreaterThan(0);
      measured.push({ inputCodeUnits: size, ...result.work });
    }
    for (const result of measured) {
      expect(result.searchedCodeUnits).toBeLessThanOrEqual(result.inputCodeUnits * 2);
    }
  });
});
