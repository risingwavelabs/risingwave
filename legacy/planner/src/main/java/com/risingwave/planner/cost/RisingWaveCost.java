package com.risingwave.planner.cost;

import com.google.common.base.Objects;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptUtil;

public class RisingWaveCost implements RisingWaveOptCost {
  private static final double MEMORY_TO_CPU_RATIO = 1.0;

  private final double rowCount;
  private final double cpu;
  private final double io;
  private final double network;
  private final double memory;

  public RisingWaveCost(double rowCount, double cpu, double io, double network, double memory) {
    this.rowCount = rowCount;
    this.cpu = cpu;
    this.io = io;
    this.network = network;
    this.memory = memory;
  }

  public double getRowCount() {
    return rowCount;
  }

  @Override
  public double getRows() {
    return rowCount;
  }

  @Override
  public double getCpu() {
    return cpu;
  }

  @Override
  public double getIo() {
    return io;
  }

  @Override
  public boolean isInfinite() {
    return this.equals(RisingWaveCostFactory.INFINITY);
  }

  @Override
  public boolean isEqWithEpsilon(RelOptCost other) {
    if (!(other instanceof RisingWaveCost)) {
      return false;
    }
    RisingWaveCost that = (RisingWaveCost) other;
    return (this == that)
        || ((Math.abs(this.cpu - that.cpu) < RelOptUtil.EPSILON)
            && (Math.abs(this.io - that.io) < RelOptUtil.EPSILON)
            && (Math.abs(this.network - that.network) < RelOptUtil.EPSILON)
            && (Math.abs(this.rowCount - that.rowCount) < RelOptUtil.EPSILON)
            && (Math.abs(this.memory - that.memory) < RelOptUtil.EPSILON));
  }

  @Override
  public boolean isLe(RelOptCost cost) {
    RisingWaveCost other = (RisingWaveCost) cost;
    return this == other || this.sumAll() <= other.sumAll();
  }

  @Override
  public boolean isLt(RelOptCost cost) {
    RisingWaveCost other = (RisingWaveCost) cost;
    return this.sumAll() < other.sumAll();
  }

  @Override
  public RelOptCost plus(RelOptCost otherCost) {
    RisingWaveCost other = (RisingWaveCost) otherCost;
    if (this.isInfinite() || other.isInfinite()) {
      return RisingWaveCostFactory.INFINITY;
    }
    return new RisingWaveCost(
        rowCount + other.rowCount,
        cpu + other.cpu,
        io + other.io,
        network + other.network,
        memory + other.memory);
  }

  @Override
  public RelOptCost minus(RelOptCost cost) {
    if (this.isInfinite()) {
      return this;
    }

    RisingWaveCost other = (RisingWaveCost) cost;
    return new RisingWaveCost(
        rowCount - other.rowCount,
        cpu - other.cpu,
        io - other.io,
        network - other.network,
        memory - other.memory);
  }

  @Override
  public RelOptCost multiplyBy(double factor) {
    if (this.isInfinite()) {
      return this;
    }

    return new RisingWaveCost(
        rowCount * factor, cpu * factor, io * factor, network * factor, memory * factor);
  }

  @Override
  public double divideBy(RelOptCost cost) {
    RisingWaveCost that = (RisingWaveCost) cost;
    double d = 1;
    double n = 0;
    if ((this.rowCount != 0)
        && !Double.isInfinite(this.rowCount)
        && (that.rowCount != 0)
        && !Double.isInfinite(that.rowCount)) {
      d *= this.rowCount / that.rowCount;
      ++n;
    }
    if ((this.cpu != 0)
        && !Double.isInfinite(this.cpu)
        && (that.cpu != 0)
        && !Double.isInfinite(that.cpu)) {
      d *= this.cpu / that.cpu;
      ++n;
    }
    if ((this.io != 0)
        && !Double.isInfinite(this.io)
        && (that.io != 0)
        && !Double.isInfinite(that.io)) {
      d *= this.io / that.io;
      ++n;
    }
    if ((this.network != 0)
        && !Double.isInfinite(this.network)
        && (that.network != 0)
        && !Double.isInfinite(that.network)) {
      d *= this.network / that.network;
      ++n;
    }

    if (n == 0) {
      return 1.0;
    }
    return Math.pow(d, 1 / n);
  }

  @Override
  public double getNetwork() {
    return network;
  }

  @Override
  public double getMemory() {
    return memory;
  }

  private double sumAll() {
    return this.cpu + this.io + this.network + this.memory * MEMORY_TO_CPU_RATIO;
  }

  @Override
  public boolean equals(RelOptCost o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RisingWaveCost that = (RisingWaveCost) o;
    return Double.compare(that.getRowCount(), getRowCount()) == 0
        && Double.compare(that.getCpu(), getCpu()) == 0
        && Double.compare(that.getIo(), getIo()) == 0
        && Double.compare(that.getNetwork(), getNetwork()) == 0
        && Double.compare(that.getMemory(), getMemory()) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getRowCount(), getCpu(), getIo(), getNetwork(), getMemory());
  }

  @Override
  public String toString() {
    return "RisingWaveCost{"
        + "rowCount="
        + rowCount
        + ", cpu="
        + cpu
        + ", io="
        + io
        + ", network="
        + network
        + ", memory="
        + memory
        + '}';
  }
}
