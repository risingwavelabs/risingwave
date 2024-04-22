#!/usr/bin/env zx

// zx: A tool for writing better scripts
// https://google.github.io/zx/

const {
  mv: mv,
  _: _command,
} = minimist(process.argv.slice(3), {
  string: ["mv", "topic"],
  _: ["list-members", "list-lags"],
});
const command = _command[0];

async function get_fragment_id_of_mv(mv_name) {
  const id = (
    await $`
    psql -h $RISEDEV_RW_FRONTEND_LISTEN_ADDRESS -p $RISEDEV_RW_FRONTEND_PORT -U root -d dev \
    --csv -t -c "select fragment_id from rw_materialized_views JOIN rw_fragments on rw_materialized_views.id = rw_fragments.table_id where name='${mv_name}';"
  `
  )
    .toString()
    .trim();
  if (id == "") {
    throw new Error(`Materialized view ${mv_name} not found`);
  }
  return id;
}

async function list_consumer_groups(fragment_id) {
  return (await $`rpk group list`)
    .toString()
    .trim()
    .split("\n")
    .slice(1)
    .map((line) => {
      const [_broker_id, group_name] = line.split(/\s+/);
      return group_name;
    })
    .filter((group_name) => {
      return group_name.startsWith(`rw-consumer-${fragment_id}`);
    });
}

async function describe_consumer_group(group_name) {
  const res = await $`rpk group describe -s ${group_name}`;
  // GROUP        rw-consumer-1-1
  // COORDINATOR  0
  // STATE        Empty
  // BALANCER
  // MEMBERS      0
  // TOTAL-LAG    2
  const obj = {};
  for (const line of res.toString().trim().split("\n")) {
    const [key, value] = line.split(/\s+/);
    obj[key] = value;
  }
  return obj;
}

async function list_consumer_group_members(fragment_id) {
  const groups = await list_consumer_groups(fragment_id);
  return Promise.all(
    groups.map(async (group_name) => {
      return (await describe_consumer_group(group_name))["MEMBERS"]
    })
  );
}

async function list_consumer_group_lags(fragment_id) {
  const groups = await list_consumer_groups(fragment_id);
  return Promise.all(
    groups.map(async (group_name) => {
      return (await describe_consumer_group(group_name))["TOTAL-LAG"]
    })
  );
}

const fragment_id = await get_fragment_id_of_mv(mv);
if (command == "list-groups") {
  echo`${(await list_consumer_groups(fragment_id))}`;
} else if (command == "list-members") {
  echo`${await list_consumer_group_members(fragment_id)}`;
} else if (command == "list-lags") {
  echo`${await list_consumer_group_lags(fragment_id)}`;
} else {
  throw new Error(`Invalid command: ${command}`);
}
