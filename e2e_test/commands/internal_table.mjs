#!/usr/bin/env zx

// zx: A tool for writing better scripts
// https://google.github.io/zx/

const {
  name: job_name,
  type: table_type,
  count: count,
} = minimist(process.argv.slice(3), {
  string: ["name", "type"],
  boolean: ["count"],
});

// Return an array of CSV string
async function psql(query) {
  return (
    await $`
psql -h $RISEDEV_RW_FRONTEND_LISTEN_ADDRESS -p $RISEDEV_RW_FRONTEND_PORT -U root -d dev \
--csv --tuples-only -c ${query}
`
  )
    .toString()
    .trim()
    .split("\n")
    .filter((line) => line.trim() != "");
}

// If `table_type` is null, return all internal tables for the job.
// If `job_name` is null, return all jobs' internal tables.
async function select_internal_table(job_name, table_type) {
  // Note: if we have `t1`, and `t1_balabala`, the latter one will also be matched 😄.
  const internal_tables = await psql(
    `select name from nim_internal_tables where name like '__internal_${job_name}_%_${table_type}_%'`
  );
  if (internal_tables.length == 0) {
    throw new Error(
      `No internal tables found for the pattern '__internal_${job_name}_%_${table_type}_%'`
    );
  }

  const res = new Map(
    await Promise.all(
      internal_tables.map(async (t) => {
        let rows = await psql(`select * from ${t}`);
        return [t, rows];
      })
    )
  );
  return res;
}

const tables = await select_internal_table(job_name, table_type);
for (const [table_name, rows] of tables) {
  if (tables.size > 1) {
    console.log(`Table: ${table_name}`);
  }
  if (count) {
    console.log(`count: ${rows.length}`);
  } else {
    if (rows.length == 0) {
      console.log("(empty)");
    } else {
      console.log(`${rows.join("\n")}`);
    }
  }
}
