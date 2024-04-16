#!/usr/bin/env zx

// zx: A tool for writing better scripts
// https://google.github.io/zx/

const { mv: mv, topic: topic, _: _command } = minimist(process.argv.slice(3), {
  string: ['mv', 'topic'],
  _: ['list-members', 'list-lags'],
})
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
  const res =
    await $`rpk group list | tail -n +2 | cut -w -f2 | grep "rw-consumer-${fragment_id}"`;
  return res;
}

async function list_consumer_group_members(fragment_id) {
  const res =
    await $`rpk group list | tail -n +2 | cut -w -f2 | grep "rw-consumer-${fragment_id}" | xargs -n1 -I {} sh -c "rpk group describe -s {} | grep "MEMBERS""`;
  return res;
}

async function list_consumer_group_lags(fragment_id, topic_name) {
  const res =
    await $`rpk group list | tail -n +2 | cut -w -f2 | grep "rw-consumer-${fragment_id}" | xargs -n1 -I {} sh -c "rpk group describe -t {} | grep "${topic_name}""`;
  return res;
}

const fragment_id = await get_fragment_id_of_mv(mv);
if (command == "list-groups") {
  echo`${await list_consumer_groups(fragment_id)}`;
} else if (command == "list-members") {
  echo`${await list_consumer_group_members(fragment_id)}`;
} else if (command == "list-lags") {
  echo`${await list_consumer_group_lags(fragment_id, topic)}`;
} else {
  throw new Error(`Invalid command: ${command}`);
}
