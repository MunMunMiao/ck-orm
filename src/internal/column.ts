import type { AnyColumn } from "../columns";

/**
 * Structural check for an `AnyColumn` — i.e. a value carrying the
 * `kind: "column"` discriminator. Centralises three near-identical
 * type guards previously duplicated in `schema.ts`, `schema-ddl.ts`,
 * and `query.ts` so the contract stays in lockstep.
 *
 * Call sites that also need to discriminate against rogue user objects
 * pretending to be columns should layer an additional check (e.g. that
 * `mapToDriverValue` is callable) on top of this guard rather than
 * reinventing a parallel one.
 */
export const isColumnLike = (value: unknown): value is AnyColumn => {
  return typeof value === "object" && value !== null && "kind" in value && value.kind === "column";
};
