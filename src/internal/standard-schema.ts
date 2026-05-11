// Minimal subset of the Standard Schema v1 specification (see
// https://github.com/standard-schema/standard-schema). ck-orm consumes any
// schema object that satisfies this shape — Zod 3.23+, Valibot 1+, ArkType,
// Effect Schema, and TypeBox all implement it natively. No external runtime
// dependency.

export interface StandardSchemaV1<Input = unknown, Output = Input> {
  readonly "~standard": StandardSchemaV1Props<Input, Output>;
}

export interface StandardSchemaV1Props<Input = unknown, Output = Input> {
  readonly version: 1;
  readonly vendor: string;
  readonly validate: (value: unknown) => StandardSchemaV1Result<Output> | Promise<StandardSchemaV1Result<Output>>;
  readonly types?: StandardSchemaV1Types<Input, Output>;
}

export interface StandardSchemaV1Types<Input, Output> {
  readonly input: Input;
  readonly output: Output;
}

export type StandardSchemaV1Result<Output> = StandardSchemaV1SuccessResult<Output> | StandardSchemaV1FailureResult;

export interface StandardSchemaV1SuccessResult<Output> {
  readonly value: Output;
  readonly issues?: undefined;
}

export interface StandardSchemaV1FailureResult {
  readonly issues: ReadonlyArray<StandardSchemaV1Issue>;
}

export interface StandardSchemaV1Issue {
  readonly message: string;
  readonly path?: ReadonlyArray<PropertyKey | StandardSchemaV1PathSegment>;
}

export interface StandardSchemaV1PathSegment {
  readonly key: PropertyKey;
}

export type InferStandardSchemaInput<TSchema extends StandardSchemaV1<unknown, unknown>> =
  TSchema extends StandardSchemaV1<infer I, unknown> ? I : never;

export type InferStandardSchemaOutput<TSchema extends StandardSchemaV1<unknown, unknown>> =
  TSchema extends StandardSchemaV1<unknown, infer O> ? O : never;

export const isStandardSchemaFailure = <Output>(
  result: StandardSchemaV1Result<Output>,
): result is StandardSchemaV1FailureResult => {
  return (result as StandardSchemaV1FailureResult).issues !== undefined;
};

export const formatStandardSchemaIssues = (issues: ReadonlyArray<StandardSchemaV1Issue>): string => {
  if (issues.length === 0) {
    return "Standard Schema validation failed";
  }
  const rendered = issues
    .map((issue) => {
      const path = issue.path
        ?.map((segment) => (typeof segment === "object" ? String(segment.key) : String(segment)))
        .join(".");
      return path ? `${path}: ${issue.message}` : issue.message;
    })
    .join("; ");
  return `Standard Schema validation failed: ${rendered}`;
};
