import { afterEach, describe, expect, it } from "bun:test";
import {
  base64EncodeUtf8,
  canSetUserAgentHeader,
  createUuid,
  hashString,
  resolveStreamRequestBodyMode,
} from "./platform";

const originalCrypto = globalThis.crypto;
const originalRequest = globalThis.Request;
const originalReadableStream = globalThis.ReadableStream;
const originalUint8Array = globalThis.Uint8Array;

const setGlobal = (name: string, value: unknown) => {
  if (value === undefined) {
    Reflect.deleteProperty(globalThis, name);
    return;
  }

  Object.defineProperty(globalThis, name, {
    configurable: true,
    writable: true,
    value,
  });
};

afterEach(function restorePlatformGlobals() {
  setGlobal("crypto", originalCrypto);
  setGlobal("Request", originalRequest);
  setGlobal("ReadableStream", originalReadableStream);
  setGlobal("Uint8Array", originalUint8Array);
});

describe("ck-orm platform", function describeClickHouseOrmPlatform() {
  it("covers UUID generation boundaries and hashing helpers", function testUuidAndHashing() {
    expect(base64EncodeUtf8("demo")).toBe("ZGVtbw==");
    expect(hashString("demo")).toBe(hashString("demo"));

    setGlobal("crypto", undefined);
    expect(() => createUuid()).toThrow("ck-orm requires Web Crypto support in the current runtime");

    setGlobal("crypto", {
      randomUUID() {
        return "uuid-from-randomUUID";
      },
    });
    expect(createUuid()).toBe("uuid-from-randomUUID");

    setGlobal("crypto", {
      getRandomValues(bytes: Uint8Array) {
        [0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x0a, 0xbc, 0x4d, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab].forEach(
          (value, index) => {
            bytes[index] = value;
          },
        );
        return bytes;
      },
    });
    expect(createUuid()).toBe("12345678-1234-4abc-8def-1234567890ab");

    class ShortBytes {
      6 = 0x0a;
      length = 8;
    }

    setGlobal("Uint8Array", ShortBytes);
    setGlobal("crypto", {
      getRandomValues(bytes: ShortBytes) {
        return bytes;
      },
    });
    expect(() => createUuid()).toThrow("Unable to generate UUID bytes");
  });

  it("covers Request-based capability probes", function testRequestProbes() {
    setGlobal("Request", undefined);
    expect(canSetUserAgentHeader()).toBe(false);
    expect(resolveStreamRequestBodyMode()).toBe("buffered");

    class ThrowingRequest {
      constructor() {
        throw new TypeError("blocked");
      }
    }

    setGlobal("Request", ThrowingRequest);
    expect(canSetUserAgentHeader()).toBe(false);
    expect(resolveStreamRequestBodyMode()).toBe("buffered");

    class PlainRequest {
      readonly headers: Headers;

      constructor(_input: string | URL | Request, init?: RequestInit) {
        this.headers = new Headers(init?.headers);
      }
    }

    setGlobal("Request", PlainRequest);
    expect(canSetUserAgentHeader()).toBe(true);
    expect(resolveStreamRequestBodyMode()).toBe("plain");

    class DuplexOnlyRequest {
      constructor(_input: string | URL | Request, init?: RequestInit & { duplex?: "half" }) {
        if (init?.duplex !== "half") {
          throw new TypeError("duplex required");
        }
      }
    }

    setGlobal("Request", DuplexOnlyRequest);
    expect(resolveStreamRequestBodyMode()).toBe("duplex-half");
  });
});
