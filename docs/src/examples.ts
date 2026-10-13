// The examples page shows each example's main.ts and main.sql exactly as they are in examples/.
// Nothing is run or generated here: the unit test next to each example (examples/<name>/main.test.ts)
// checks that main.sql is the SQL its main.ts sends.
export type Example = {
  /** Folder name in examples/, also the anchor on the examples page. */
  id: string;
  /** Repository path of the runnable file. */
  path: string;
  source: string;
  sql: string;
};

const read = async (path: string) => (await Bun.file(new URL(`../../${path}`, import.meta.url)).text()).trimEnd();

const example = async (id: string): Promise<Example> => ({
  id,
  path: `examples/${id}/main.ts`,
  source: await read(`examples/${id}/main.ts`),
  sql: await read(`examples/${id}/main.sql`),
});

// Written by hand, in display order.
export const examples = [await example("decimal-sum"), await example("left-join"), await example("literal-search")];
