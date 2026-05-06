const uuidVersionMask = 0x40;
const uuidVariantMask = 0x80;
const fnvPrime = 0x01000193;
const fnvOffsetBasis = 0x811c9dc5;
const base64Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

// `TextEncoder` is stateless — caching one at module load lets every
// `base64EncodeUtf8` (per Auth header) and `hashString` (per query event)
// skip the constructor.
const utf8Encoder = new TextEncoder();

const getCryptoApi = () => {
  if (typeof globalThis.crypto === "undefined") {
    throw new Error("ck-orm requires Web Crypto support in the current runtime");
  }
  return globalThis.crypto;
};

const bytesToHex = (bytes: Uint8Array) => {
  let hex = "";
  for (const byte of bytes) {
    hex += byte.toString(16).padStart(2, "0");
  }
  return hex;
};

export const createUuid = () => {
  const cryptoApi = getCryptoApi();
  if (typeof cryptoApi.randomUUID === "function") {
    return cryptoApi.randomUUID();
  }

  const bytes = new Uint8Array(16);
  cryptoApi.getRandomValues(bytes);
  const versionByte = bytes[6];
  const variantByte = bytes[8];
  if (versionByte === undefined || variantByte === undefined) {
    throw new Error("Unable to generate UUID bytes");
  }
  bytes[6] = (versionByte & 0x0f) | uuidVersionMask;
  bytes[8] = (variantByte & 0x3f) | uuidVariantMask;

  const hex = bytesToHex(bytes);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
};

export const base64EncodeUtf8 = (value: string) => {
  const bytes = utf8Encoder.encode(value);
  let encoded = "";

  for (let index = 0; index < bytes.length; index += 3) {
    const a = bytes[index] ?? 0;
    const b = bytes[index + 1] ?? 0;
    const c = bytes[index + 2] ?? 0;
    const triple = (a << 16) | (b << 8) | c;

    encoded += base64Alphabet[(triple >> 18) & 0x3f];
    encoded += base64Alphabet[(triple >> 12) & 0x3f];
    encoded += index + 1 < bytes.length ? base64Alphabet[(triple >> 6) & 0x3f] : "=";
    encoded += index + 2 < bytes.length ? base64Alphabet[triple & 0x3f] : "=";
  }

  return encoded;
};

export const hashString = (value: string) => {
  const bytes = utf8Encoder.encode(value);
  let hash = fnvOffsetBasis;

  for (const byte of bytes) {
    hash ^= byte;
    hash = Math.imul(hash, fnvPrime) >>> 0;
  }

  return hash.toString(16).padStart(8, "0");
};

export const canSetUserAgentHeader = () => {
  if (typeof Request === "undefined") {
    return false;
  }

  try {
    const request = new Request("http://localhost/", {
      headers: {
        "User-Agent": "ck-orm-probe",
      },
    });
    return request.headers.get("User-Agent") === "ck-orm-probe";
  } catch {
    return false;
  }
};

export type StreamRequestBodyMode = "plain" | "duplex-half" | "buffered";

const createProbeStream = () => {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.close();
    },
  });
};

// The runtime's stream-body capability is fixed for the lifetime of the
// process; probing once is enough. Lazy so that environments without
// `Request` (used at module-load by `canSetUserAgentHeader`) don't pay the
// cost unless an `insertJsonEachRow(asyncIterable)` actually fires.
let cachedStreamRequestBodyMode: StreamRequestBodyMode | undefined;

/**
 * Test-only reset for the stream-body probe cache. Production code never
 * needs to invalidate the cache because runtime capabilities are stable for
 * the process lifetime; tests that swap `globalThis.Request` between
 * scenarios call this to force a re-probe.
 */
export const _resetStreamRequestBodyModeForTest = () => {
  cachedStreamRequestBodyMode = undefined;
};

export const resolveStreamRequestBodyMode = (): StreamRequestBodyMode => {
  if (cachedStreamRequestBodyMode !== undefined) {
    return cachedStreamRequestBodyMode;
  }

  if (typeof Request === "undefined" || typeof ReadableStream === "undefined") {
    cachedStreamRequestBodyMode = "buffered";
    return cachedStreamRequestBodyMode;
  }

  try {
    new Request("http://localhost/", {
      method: "POST",
      body: createProbeStream(),
    });
    cachedStreamRequestBodyMode = "plain";
    return cachedStreamRequestBodyMode;
  } catch {
    try {
      new Request("http://localhost/", {
        method: "POST",
        body: createProbeStream(),
        duplex: "half",
      } as RequestInit & { duplex: "half" });
      cachedStreamRequestBodyMode = "duplex-half";
      return cachedStreamRequestBodyMode;
    } catch {
      cachedStreamRequestBodyMode = "buffered";
      return cachedStreamRequestBodyMode;
    }
  }
};
