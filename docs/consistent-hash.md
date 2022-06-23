# Consistent Hash

## Background

Scaling could occur for multiple reasons in RisingWave. For example, when workload of some streaming operator is heavier than expected, we need to scale out. During scaling out or scaling in, physical resources will be allocated or freed accordingly. That is to say, new [actors](./streaming-overview.md#actors) will be created on scaling out, while some old actors will be dropped on scaling in. As a result of actor change, redistributing data (i.e. states of the actors) is inevitably required. That yields a question: how to efficiently determine the distribution of data and minimize data movement on scaling?

On the other hand, we need to parallel the scan on tables or materialized views in [batch query mode](./architecture-design.md#batch-query-mode). Therefore, we need to partition the data in a way that could boost the performance most. So what is the most beneficial way of data partition for tables and materialized views?

In RisingWave, we adopt consistent-hash-based strategy to solve the two problems above. This document will elaborate our design.

## Design

### Actor Scheduling

First, we need to introduce a little about how we schedule the actors. Each worker node in RisingWave cluster will have a number of parallel units, which could be interpreted as CPU cores. Parallel unit is the minimal scheduling unit in RisingWave. Each actor will be scheduled to exactly one parallel unit.

### Consistent Hash Mapping

Then comes the consistent hash part, where we will construct a mapping that determines data distribution.

For all data $k \in U_k$, we apply a hash function $v = H(k)$, where $v$ falls to a limited range. The hash function ensures that all $k$ are hashed **uniformly** to that range. We call $v$ vnode, as is shown as the squares in the figure below.

All vnodes are mapped to parallel units in the cluster evenly. As is shown in the figure below, we have $3$ parallel units, each taking $\frac{1}{3}$ of total vnodes. The vnode mapping is constructed and maintained by [meta](./architecture-design.md#architecture).

<!-- Now we have vnodes corresponding to disjoint sets of keys, which naturally forms a data partition pattern. That is to say, one vnode could be viewed as a minimal data partition unit, and we could aggregate several vnodes together to get a larger data partition. -->

<!-- In order to determine the distribution of data, we need to devise a way to physically aggregate the data. So we have parallel unit to represent the physical location of data, as is shown as the circles in the figure below. A parallel unit is also the minimal scheduling unit. That is to say, one actor could be scheduled to exactly one parallel unit, and all data (i.e. states) of the actor will. -->

Since $v = H(k)$, the mapping from data to vnodes will be invariant. Therefore, when scaling occurs, we only need to modify vnode mapping, so as to redistribute the data.

![initial data distribution](./images/consistent-hash/initial-data-distribution.svg)

### Scaling

Let's take scaling out for example, as is shown in the figure below. Assume that we have one more parallel unit after scaling out, as is depicted as the orange circle. Based on consistent hash strategy, we modify the vnode mapping in a way that ... The left subgraph shows the data distribution after scaling out.

![data redistribution after scaling](./images/consistent-hash/data-redistribution.svg)
<!-- 
### Data Movement

#### Actor

#### Block Cache -->
