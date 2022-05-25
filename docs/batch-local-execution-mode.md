# Design: Local execution mode for online serving queries

## Background

The query engine of RisingWave supports two types of queries: highly concurrent point queries and ad-hoc queries. The characteristics of these two different kinds of queries are summarized as followings:

| 	              | Point Queries	 | Adhoc Queries	        |
|----------------|----------------|-----------------------|
| Latency	       | several ms	    | ms to minutes	        |
| QPS	           | 1000 ~ 100000	 | 100 ~ 1000	           |
| SQL	           | Simple	        | Arbitrary complex	    |
| Result Set	    | Small	         | Small, Medium, Large	 |
| Use Scenarios	 | Dashboard	     | Adhoc analysis	       |

Our distributed query processing engine is designed for complex adhoc queries, and it can’t meet the latency/qps 
requirement of point queries, and in this article we introduce local execution mode for point queries.

## Design

![Frontend Flow](./images/batch-local-execution-mode/frontend-flow.svg)

## Example 1: select a from t where b in (1, 2, 3, 4)

Let’s use the above SQL as an example:


![Example 1](./images/batch-local-execution-mode/example1.svg)

The key changes from the distributed mode:

1. The exchange executor will be executed directly by local query execution, not by distributed scheduler. This means that we no longer have async execution/monitoring, etc.
2. The rpc is issued by exchange executor directly, not by scheduler.

## Example 2: SELECT pk, t1.a, t1.fk, t2.b FROM t1, t2 WHERE t1.fk = t2.pk AND t1.pk = 114514

Following is the plan and execution of above sql in local mode:


![Example 2](./images/batch-local-execution-mode/example2.svg)

As explained above, the lookup join/exchange phase will be executed directly on frontend. The pushdown(filter/table, both the build and probe side) will be issued by executors rather than scheduler.

### Optimization/Scheduling

The overall process will be quite similar to distributed processing, but with a little difference:

1. We only use heuristic optimizer for it, and only a limited set of rules will be applied.
2. The scheduler will not be involved, and the physical plan is executed in the current thread (coroutine) immediately.

### Monitoring/Management

Local execution mode will not go through query management mentioned in  batch query manager to reduce latency as 
much as possible.

### How to switch between local/distributed execution modes?

As mentioned in the first paragraph, the main use case for local execution mode is determined(dashboard/reporting), so 
currently we just expose a session configuration(`query_mode`) to user. In future we may use optimizer to determined 
it if required.

### RPC execution in local mode

In the distributed mode we have several steps to execute a computing task and fetch results:

There are some problems with above process in local mode:

1. We need at least two rpcs to fetch task execution result, this increases query overhead
2. We have task lifecycle management APIs, this is unnecessary for local mode.
3. We may need to add several new APIs for task monitoring/failure detection


For the local mode we will add a new rpc API:

```
rpc Execute(ExecuteRequest) returns (ExecuteResponse)

message ExecuteRequest {
 batch_plan.PlanFragment plan = 2;
 uint64 epoch = 3;
}

message ExecuteResponse {
  common.Status status = 1;
  data.DataChunk record_batch = 2;
}
```

This is quite similar to distributed execution APIs, but with some differences:

1. Response is returned directly in rpc call, without the need of another call.
2. No task lifecycle management/monitoring is involved, and if it fails, we just remove the task and return error in response directly.
