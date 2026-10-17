import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import { ckTable, ckType, clickhouseClient } from "../public_api";
import { prettyRows } from "./pretty";
import {
  makeWorkload,
  measureTrimWork,
  offlineClient,
  trimEndDescriptor,
  workloadTable,
} from "./pretty-performance-support.test";

const ORIGINAL_HEAD_FORMATTER_DIGEST = "d7a06ca2f42bc919148c0c5f7e6fc7f75509d02c2be4593b7166d16c95889f90";
const originalTrimEnd = trimEndDescriptor();

const layouts = [
  ["row-local trailing whitespace", "  (1, ' a  ') \t, \r\n (2, ' b ') \t ", "  (1, ' a  '),\n(2, ' b ')"],
  [
    "nested literals",
    "(1,['a,b','(x)'],(2,3),{'a':4}), (2,[],(5,6),{})",
    "(1,['a,b','(x)'],(2,3),{'a':4}),\n(2,[],(5,6),{})",
  ],
  [
    "single-quote escapes",
    String.raw`(1,'a\'b, (c) \\ d'), (2,'it''s , (ok)  ')`,
    String.raw`(1,'a\'b, (c) \\ d'),
(2,'it''s , (ok)  ')`,
  ],
  [
    "double quotes and backticks",
    '(1,"a, (b)  ",`x,y`,"it""s"), (2,"a\\"b",`it``s`)',
    '(1,"a, (b)  ",`x,y`,"it""s"),\n(2,"a\\"b",`it``s`)',
  ],
  [
    "literal newlines and Unicode spaces",
    "(1,'a\n\t b, (雪)  '),  (2,' c\r\n') ",
    "(1,'a\n\t b, (雪)  '),\n(2,' c\r\n')",
  ],
  ["single row", "(1, 'a,b')  ", "(1, 'a,b')"],
  ["whitespace-only input", " \n\t ", ""],
  ["empty input", "", ""],
  ["consecutive top-level separators", "(1), , \t (2),  ", "(1),,\n(2),"],
] as const;

const unsupported = [
  "(1,'open), (2,'x')",
  '(1,"open), (2,"x")',
  "(1,`open), (2,`x`)",
  "(1,2), (3,4",
  "(1,2)) , (3,4)",
  "(1) -- keep newline\n, (2)",
  "(1) /* keep comment */ , (2)",
  "(1) # keep comment\n, (2)",
  "(1,$$a,b$$), (2,'ok')",
];

const whitespace = [
  0x0009, 0x000b, 0x000c, 0x0020, 0x00a0, 0xfeff, 0x1680, 0x2000, 0x2001, 0x2002, 0x2003, 0x2004, 0x2005, 0x2006,
  0x2007, 0x2008, 0x2009, 0x200a, 0x202f, 0x205f, 0x3000, 0x000a, 0x000d, 0x2028, 0x2029,
].map((code) => String.fromCharCode(code));

const opaqueAtoms = [
  "(",
  ")",
  "[",
  "]",
  "{",
  "}",
  ",",
  "a",
  "0",
  ":",
  " ",
  "\r\n",
  String.fromCharCode(0x2028),
  String.fromCharCode(0xfeff),
  "'x, y '",
  '"x""y"',
  "`x``y`",
  "'--#$$'",
  "\u0085",
  String.fromCharCode(0x200b),
  "\ud800",
  "\udfff",
  "/",
  "*",
  "-",
  "$",
  "#",
];

describe("PERF-005 prettyRows trim work", () => {
  it("PERF-005 RED: public pretty INSERT bounds cumulative trim input linearly", () => {
    const results = [128, 256, 512].map((rows) => {
      const { builder, plainBody, prettyBody } = makeWorkload(rows);
      const plain = measureTrimWork(() => builder.toSQL());
      const pretty = measureTrimWork(() => builder.toSQL({ pretty: true }));
      expect(trimEndDescriptor()).toEqual(originalTrimEnd);
      expect(plain.value.body).toBe(plainBody);
      expect(pretty.value.body).toBe(prettyBody);
      expect(pretty.value.params).toEqual(plain.value.params);
      expect(pretty.value.forcedSettings).toEqual(plain.value.forcedSettings);
      return { rows, bodyChars: plainBody.length, plain: plain.work, pretty: pretty.work };
    });
    for (const result of results) {
      expect(result.pretty.receiverChars, `rows=${result.rows}: repeated accumulated-prefix trim`).toBeLessThanOrEqual(
        4 * result.bodyChars,
      );
    }
    const first = results[0];
    const last = results[2];
    if (first === undefined || last === undefined) {
      throw new Error("missing PERF-005 trim samples");
    }
    expect(last.pretty.receiverChars).toBeLessThanOrEqual(5 * Math.max(1, first.pretty.receiverChars));
  });

  it("PERF-005 CONTROL: plain and pretty:false keep the original literal body without cumulative trim", () => {
    const { builder, plainBody } = makeWorkload(512);
    const plain = measureTrimWork(() => builder.toSQL());
    expect(trimEndDescriptor()).toEqual(originalTrimEnd);
    expect(plain.value.sql).toBe("insert into `audit_perf005_values` (`id`, `text`) format Values");
    expect(plain.value.body).toBe(plainBody);
    expect(plain.value.params).toEqual({});
    expect(plain.value.forcedSettings).toEqual({
      input_format_values_interpret_expressions: 0,
      input_format_values_deduce_templates_of_expressions: 0,
    });
    expect(builder.toSQL({ pretty: false })).toEqual(plain.value);
    expect(plain.work.receiverChars).toBeLessThanOrEqual(plainBody.length);
  });
});

describe("PERF-005 prettyRows independent layout and fallback", () => {
  for (const [label, input, expected] of layouts) {
    it(`PERF-005 CONTROL: prettyRows preserves ${label}`, () => {
      expect(prettyRows(input)).toBe(expected);
    });
  }

  for (const body of unsupported) {
    it(`PERF-005 CONTROL: unsupported body returns byte-exact original ${JSON.stringify(body)}`, () => {
      expect(prettyRows(body)).toBe(body);
    });
  }

  it("PERF-005 CONTROL: independent oracle preserves every whitespace class inside quotes and trims only layout tails", () => {
    for (const space of whitespace) {
      const first = `(1,'${space},()[]{}${space}',[1,2])`;
      const second = `(2,"${space}x, y${space}",\`a${space}b\`)`;
      expect(prettyRows(`${space}${first}${space},${space}${second}${space}`)).toBe(`${space}${first},\n${second}`);
      expect(prettyRows(`${space},${space},${space}`)).toBe(",,");
      expect(prettyRows(`${first},${space},${space}${second},${space}`)).toBe(`${first},,\n${second},`);
    }
    for (const code of [0x0085, 0x180e, 0x200b, 0x2060]) {
      const char = String.fromCharCode(code);
      expect(prettyRows(`(1), (2)${char} `)).toBe(`(1),\n(2)${char}`);
    }
  });

  it("PERF-005 CONTROL: constructed nested literals have independently known complete output", () => {
    const literals = [
      "''",
      "'  雪,()[]{}  '",
      "'it''s'",
      String.raw`'a\'b\\tail'`,
      String.raw`"a\"b, c"`,
      '"a""b"',
      "`a``b, c`",
      "'line\n\t\r\nend '",
      "'-- /* # $$ , ('",
      "'\u0000𐏿'",
    ];
    for (let index = 0; index < 256; index += 1) {
      const space = whitespace[index % whitespace.length] as string;
      const rows = Array.from({ length: 1 + (index % 5) }, (_, row) => {
        const literal = literals[(index + row) % literals.length];
        return `(${index + row},${space}${literal},[1,${literal}],(2,3),{'x,y':4})`;
      });
      const input = space + rows.join(`${space},${space}`) + space;
      expect(prettyRows(input)).toBe(space + rows.join(",\n"));
    }
  });

  it("PERF-005 CONTROL: fallback remains byte-exact after partially formatted prefixes", () => {
    const prefix = " (1, '  a  '), \t (2, [3,4]), \n";
    for (const suffix of [
      "('unterminated",
      '("unterminated',
      "(`unterminated",
      "(3, [4] ",
      "(3, 4))  ",
      "-- end\n(3)",
      "/* x */ (3)",
      "# end\n(3)",
      "($tag$x,y$tag$)",
    ]) {
      const input = prefix + suffix;
      expect(prettyRows(input)).toBe(input);
    }
  });

  it("PERF-005 CONTROL: 4096 opaque inputs match the immutable original-HEAD formatter digest", () => {
    let state = 0x005a11ce;
    const next = () => {
      state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
      return state;
    };
    const pairs: [string, string][] = [];
    for (let index = 0; index < 4096; index += 1) {
      const input = Array.from({ length: 1 + (next() % 32) }, () => opaqueAtoms[next() % opaqueAtoms.length]).join("");
      pairs.push([input, prettyRows(input)]);
    }
    expect(pairs).toHaveLength(4096);
    expect(createHash("sha256").update(JSON.stringify(pairs)).digest("hex")).toBe(ORIGINAL_HEAD_FORMATTER_DIGEST);
  });
});

describe("PERF-005 public INSERT pretty and execute body identity", () => {
  it("PERF-005 CONTROL: public pretty INSERT preserves complex literals and exact quoted whitespace", () => {
    const table = ckTable("audit_perf005_complex", {
      id: ckType.uint32(),
      text: ckType.string(),
      tags: ckType.array(ckType.string()),
      pair: ckType.tuple(ckType.int32(), ckType.string()),
      attributes: ckType.map(ckType.string(), ckType.int32()),
      payload: ckType.json(),
      optional: ckType.nullable(ckType.string()),
      amount: ckType.decimal({ precision: 12, scale: 2 }),
      big: ckType.uint64(),
    });
    const builder = offlineClient()
      .insert(table)
      .values([
        {
          id: 1,
          text: "  it's (a,b)\\tail\n\t  ",
          tags: ["a,b", " (x) "],
          pair: [7, "p,q"],
          attributes: { "a,b": 9 },
          payload: { k: "a, (b)" },
          optional: null,
          amount: "12.34",
          big: "9007199254740993",
        },
        {
          id: 2,
          text: '"quoted",\r\n雪 ',
          tags: [],
          pair: [8, "  "],
          attributes: {},
          payload: { k: "x" },
          optional: " end ",
          amount: "0.01",
          big: "4",
        },
      ]);
    const first = String.raw`(1,'  it\'s (a,b)\\tail\n\t  ',['a,b',' (x) '],(7,'p,q'),{'a,b':9},'{"k":"a, (b)"}',NULL,12.34,9007199254740993)`;
    const second = "(2,'\"quoted\",\\r\\n雪 ',[],(8,'  '),{},'{\"k\":\"x\"}',' end ',0.01,4)";
    const plain = builder.toSQL();
    const pretty = builder.toSQL({ pretty: true });
    expect(plain.body).toBe(`${first}, ${second}`);
    expect(pretty.body).toBe(`${first},\n${second}`);
    expect(pretty.params).toEqual(plain.params);
    expect(pretty.forcedSettings).toEqual(plain.forcedSettings);
    expect(builder.toSQL()).toEqual(plain);
  });

  it("PERF-005 CONTROL: SDK execute sends original body before and after pretty inspection (in-memory transport)", async () => {
    const sent: { query: string | null; body: string }[] = [];
    const db = clickhouseClient({
      host: "http://127.0.0.1:1",
      fetch: async (request) => {
        sent.push({ query: new URL(request.url).searchParams.get("query"), body: await request.text() });
        return new Response("");
      },
    });
    const builder = db.insert(workloadTable()).values([
      { id: 1, text: "  a, (b)  " },
      { id: 2, text: "it's\\ok\n" },
    ]);
    const raw = String.raw`(1,'  a, (b)  '), (2,'it\'s\\ok\n')`;
    await builder.execute();
    expect(builder.toSQL({ pretty: true }).body).toBe(String.raw`(1,'  a, (b)  '),
(2,'it\'s\\ok\n')`);
    await builder.execute();
    expect(sent).toEqual([
      { query: "insert into `audit_perf005_values` (`id`, `text`) format Values", body: raw },
      { query: "insert into `audit_perf005_values` (`id`, `text`) format Values", body: raw },
    ]);
  });

  it("PERF-005 CONTROL: alternating public builders recompute independent plain and pretty bodies", () => {
    const parent = offlineClient().insert(workloadTable()).values({ id: 1, text: " a, b " });
    const branch = parent.values({ id: 2, text: " c, d " });
    const unrelated = offlineClient().insert(workloadTable()).values({ id: 3, text: " e, f " });
    for (let repeat = 0; repeat < 4; repeat += 1) {
      expect(branch.toSQL({ pretty: true }).body).toBe("(1,' a, b '),\n(2,' c, d ')");
      expect(unrelated.toSQL({ pretty: true }).body).toBe("(3,' e, f ')");
      expect(parent.toSQL({ pretty: true }).body).toBe("(1,' a, b ')");
      expect(branch.toSQL().body).toBe("(1,' a, b '), (2,' c, d ')");
      expect(parent.toSQL().body).toBe("(1,' a, b ')");
    }
  });
});
