import { createClientValidationError } from "../errors";

/**
 * Shared integer validation helpers.
 *
 * `columns.ts` and `functions.ts` previously each shipped their own
 * positive-integer / non-negative-integer / 0..9-range assertions with
 * subtly different `typeof` handling and error wording. Centralising them
 * here keeps the contract — and the diagnostic message — in one place.
 *
 * All assertions accept `unknown` and narrow via `asserts value is number`,
 * so call sites can drop the upstream `typeof` guard.
 */

export const assertPositiveInteger: (label: string, value: unknown) => asserts value is number = (label, value) => {
  if (typeof value !== "number" || !Number.isInteger(value) || value <= 0) {
    throw createClientValidationError(`${label} must be a positive integer, got ${String(value)}`);
  }
};

export const assertNonNegativeInteger: (label: string, value: unknown) => asserts value is number = (label, value) => {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throw createClientValidationError(`${label} must be a non-negative integer, got ${String(value)}`);
  }
};

export const assertIntegerInRange: (
  label: string,
  value: unknown,
  min: number,
  max: number,
) => asserts value is number = (label, value, min, max) => {
  if (typeof value !== "number" || !Number.isInteger(value) || value < min || value > max) {
    throw createClientValidationError(`${label} must be an integer between ${min} and ${max}, got ${String(value)}`);
  }
};
