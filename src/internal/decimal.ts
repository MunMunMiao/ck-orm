import { createClientValidationError } from "../errors";

export const DECIMAL_MAX_PRECISION = 76;

export type DecimalParams = {
  readonly precision: number;
  readonly scale: number;
};

export const assertDecimalParams = (params: DecimalParams, label = "decimal"): void => {
  const { precision, scale } = params;
  if (!Number.isInteger(precision) || precision < 1 || precision > DECIMAL_MAX_PRECISION) {
    throw createClientValidationError(
      `${label} precision must be an integer between 1 and ${DECIMAL_MAX_PRECISION}, got ${precision}`,
    );
  }
  if (!Number.isInteger(scale) || scale < 0 || scale > precision) {
    throw createClientValidationError(
      `${label} scale must be an integer between 0 and precision (${precision}), got ${scale}`,
    );
  }
};

export const formatDecimalSqlType = (params: DecimalParams): string => {
  return `Decimal(${params.precision}, ${params.scale})`;
};

const FIXED_WIDTH_PRECISION: Record<string, number> = {
  Decimal32: 9,
  Decimal64: 18,
  Decimal128: 38,
  Decimal256: 76,
};

const FIXED_WIDTH_DECIMAL_PATTERN = /^(Decimal(?:32|64|128|256))\s*\(\s*(\d+)\s*\)$/;
const PARAMETERISED_DECIMAL_PATTERN = /^Decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$/;

/**
 * Parse a ClickHouse Decimal sqlType string. Returns `undefined` for any
 * non-decimal or malformed input. Validates that `0 ≤ scale ≤ precision`
 * so that downstream code can trust the returned `DecimalParams` without
 * re-running `assertDecimalParams`.
 *
 * Recognised forms:
 *   - `Decimal(P, S)`           — explicit precision / scale
 *   - `Decimal32(S)`            — fixed P=9
 *   - `Decimal64(S)`            — fixed P=18
 *   - `Decimal128(S)`           — fixed P=38
 *   - `Decimal256(S)`           — fixed P=76
 */
export const parseDecimalSqlType = (sqlType: string | undefined): DecimalParams | undefined => {
  if (!sqlType) return undefined;
  const trimmed = sqlType.trim();

  const fixedMatch = trimmed.match(FIXED_WIDTH_DECIMAL_PATTERN);
  if (fixedMatch) {
    const head = fixedMatch[1];
    if (!head) return undefined;
    const precision = FIXED_WIDTH_PRECISION[head];
    if (precision === undefined) return undefined;
    const scale = Number(fixedMatch[2]);
    if (!Number.isInteger(scale) || scale < 0 || scale > precision) return undefined;
    return { precision, scale };
  }

  const paramMatch = trimmed.match(PARAMETERISED_DECIMAL_PATTERN);
  if (paramMatch) {
    const precision = Number(paramMatch[1]);
    const scale = Number(paramMatch[2]);
    if (
      !Number.isInteger(precision) ||
      !Number.isInteger(scale) ||
      precision < 1 ||
      precision > DECIMAL_MAX_PRECISION ||
      scale < 0 ||
      scale > precision
    ) {
      return undefined;
    }
    return { precision, scale };
  }

  return undefined;
};
