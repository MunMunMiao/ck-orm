import type { Column } from "../index";

export type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
export type Expect<T extends true> = T;
export type DataOf<TValue> = TValue extends Column<infer TData> ? TData : never;
export type InferBuilderResult<TValue> = Awaited<TValue> extends Array<infer TResult> ? TResult : never;
