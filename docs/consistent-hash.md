# Consistent Hash

## Background

Scaling could occur for multiple reasons in RisingWave. For example, when workload of some streaming operator is heavier than expected, we need to scale out. During scaling out or scaling in, physical resources will be allocated or freed accordingly. That is to say, new [actors](./streaming-overview.md#actors) will be created on scaling out, while some old actors will be dropped on scaling in. As a result of actor change, redistributing data (i.e. states of the actors) is required inevitably. That yields a question: how to efficiently determine the distribution of data and minimize data movement on scaling?

On the other hand, we need to parallel the scan on tables or materialized views in [batch query mode](./architecture-design.md#batch-query-mode). Therefore, we need to partition the data in a way that could boost the performance most. So what is the most beneficial way of data partition for tables and materialized views?

In RisingWave, we adopt consistent-hash-based strategy to solve the two problems above. This document will elaborate our design.

## Design

### Meta

#### Actor Scheduling

First, we need to introduce a little about how we schedule the actors. Each worker node in RisingWave cluster will have a number of parallel units. A parallel unit is the minimal scheduling unit in RisingWave, as well as the physical location of an actor. Each actor will be scheduled to exactly one parallel unit.

#### Data Distribution

Here comes the main part, where we will construct a mapping that determines data distribution.

For all data $k \in U_k$, where $U_k$ is an unbounded set, we apply a hash function $v = H(k)$, where $v$ falls to a limited range. The hash function $H$ ensures that all data are hashed **uniformly** to that range. We call $v$ vnode, as is shown as the squares in the figure below.

![initial data distribution](./images/consistent-hash/data-distribution.svg)

Then we have vnode mapping, which ensures that vnodes are mapped evenly to parallel units in the cluster. That is to say, the number of vnodes that are mapped to each parallel unit should be as close as possible. This is denoted by different colors in the figure above. As is depicted, we have 3 parallel units (shown as circles), each taking $\frac{1}{3}$ of total vnodes. Vnode mapping is [constructed and maintained by meta]([./architecture-design.md#architecture](https://github.com/singularity-data/risingwave/blob/main/src/meta/src/manager/hash_mapping.rs)).

As long as the hash function $H$ could ensure uniformity, the data distribution determined by this strategy would be even across physical resources, even if data in $U_k$ might skew to a certain range. 

<!-- Now we have vnodes corresponding to disjoint sets of keys, which naturally forms a data partition pattern. That is to say, one vnode could be viewed as a minimal data partition unit, and we could aggregate several vnodes together to get a larger data partition. -->

<!-- In order to determine the distribution of data, we need to devise a way to physically aggregate the data. So we have parallel unit to represent the physical location of data, as is shown as the circles in the figure below. A parallel unit is also the minimal scheduling unit. That is to say, one actor could be scheduled to exactly one parallel unit, and all data (i.e. states) of the actor will. -->

#### Data Redistribution

Since $v = H(k)$, the way that data are mapped to vnodes will be invariant. Therefore, when scaling occurs, we only need to modify vnode mapping (the way that vnodes are mapped to parallel units), so as to redistribute the data.

Let's take scaling out for example. Assume that we have one more parallel unit after scaling out, as is depicted as the orange circle in the figure below. Using the optimal strategy, we modify the vnode mapping in such a way that only $\frac{1}{4}$ of the data have to be moved, as is shown in the figure below. The vnodes whose data are required to be moved are highlighted with bold border in the figure.

![optimal data redistribution](./images/consistent-hash/data-redistribution-1.svg)

To minimize data movement when scaling occurs, we should be careful when we modify the vnode mapping. The worst strategy will result in $\frac{1}{2}$ of the data being moved, just as is shown in the figure below.

![worst data redistribution](./images/consistent-hash/data-redistribution-2.svg)

### Streaming

We know that a fragment has several actors as its different parallelisms, and that upstream actors will send data to downstream actors via [dispatcher](./streaming-overview.md#actors). The figure below illustrates how actors distribute data based on consistent hash by example. 

![actor data distribution](./images/consistent-hash/actor-data.svg)

In the figure, we can see that one upstream actor dispatches data to three downstream actors. The downstream actors are scheduled on the parallel units mentioned in previous example respectively. 

Based on our consistent hash design, the dispatcher is informed of latest vnode mapping by meta. It then decides how to send data in following steps:
1. Compute vnode of the data via the hash function $H$. Let the vnode be $v_k$.
2. Look up vnode mapping and find out parallel unit $p_n$ that vnode $v_k$ maps to. 
3. Send data to the downstream actor that is scheduled on this parallel unit $p_n$ (remember that one actor will be scheduled on exactly one parallel unit).

In this way, all actors' data (i.e. actors' states) are distributed according to the vnode mapping constructed by meta. By determining the construction and modification of vnode mapping, we could hit the target to minimize data movement on scaling.

### Storage


### Batch


<!-- 
### Data Movement

#### Actor

#### Block Cache -->
