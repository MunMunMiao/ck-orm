import { describe, expect, it } from "bun:test";
import { assertJsonPathIdentifier, parseJsonPathSegments } from "./json-path";

describe("ck-orm internal JSON path", function describeJsonPath() {
  describe("parseJsonPathSegments", function describeParseJsonPathSegments() {
    it("splits valid dotted paths into segments", function testValidPaths() {
      expect(parseJsonPathSegments("a")).toEqual(["a"]);
      expect(parseJsonPathSegments("a.b")).toEqual(["a", "b"]);
      expect(parseJsonPathSegments("a.b.c")).toEqual(["a", "b", "c"]);
      expect(parseJsonPathSegments("user_id")).toEqual(["user_id"]);
      expect(parseJsonPathSegments("_internal._meta")).toEqual(["_internal", "_meta"]);
      expect(parseJsonPathSegments("a1.b2.c3")).toEqual(["a1", "b2", "c3"]);
    });
  });

  describe("assertJsonPathIdentifier", function describeAssertJsonPathIdentifier() {
    it("accepts valid identifiers", function testValidIdentifiers() {
      expect(() => assertJsonPathIdentifier("a")).not.toThrow();
      expect(() => assertJsonPathIdentifier("a.b")).not.toThrow();
      expect(() => assertJsonPathIdentifier("a.b.c")).not.toThrow();
      expect(() => assertJsonPathIdentifier("user_id")).not.toThrow();
      expect(() => assertJsonPathIdentifier("nested.score")).not.toThrow();
    });

    it("rejects empty input", function testEmptyInput() {
      expect(() => assertJsonPathIdentifier("")).toThrow(/non-empty/);
    });

    it("rejects empty segments", function testEmptySegments() {
      expect(() => assertJsonPathIdentifier(".a")).toThrow(/empty segment/);
      expect(() => assertJsonPathIdentifier("a.")).toThrow(/empty segment/);
      expect(() => assertJsonPathIdentifier("a..b")).toThrow(/empty segment/);
    });

    it("rejects whitespace", function testWhitespace() {
      expect(() => assertJsonPathIdentifier(" a")).toThrow(/Invalid SQL identifier/);
      expect(() => assertJsonPathIdentifier("a b")).toThrow(/Invalid SQL identifier/);
      expect(() => assertJsonPathIdentifier("a\tb")).toThrow(/Invalid SQL identifier/);
    });

    it("rejects SQL-dangerous characters", function testSqlDangerousChars() {
      expect(() => assertJsonPathIdentifier("a';--")).toThrow(/Invalid SQL identifier/);
      expect(() => assertJsonPathIdentifier('a"b')).toThrow(/Invalid SQL identifier/);
      expect(() => assertJsonPathIdentifier("a`b")).toThrow(/Invalid SQL identifier/);
      expect(() => assertJsonPathIdentifier("a-b")).toThrow(/Invalid SQL identifier/);
      expect(() => assertJsonPathIdentifier("a/b")).toThrow(/Invalid SQL identifier/);
    });

    it("rejects segments starting with a digit", function testLeadingDigit() {
      expect(() => assertJsonPathIdentifier("1a")).toThrow(/Invalid SQL identifier/);
      expect(() => assertJsonPathIdentifier("a.2b")).toThrow(/Invalid SQL identifier/);
    });
  });
});
