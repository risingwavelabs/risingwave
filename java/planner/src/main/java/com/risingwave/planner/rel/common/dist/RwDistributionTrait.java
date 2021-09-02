package com.risingwave.planner.rel.common.dist;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;
import static org.apache.calcite.rel.RelDistributions.ANY;

import com.google.common.base.Objects;
import com.google.common.collect.Ordering;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.calcite.plan.RelMultipleTrait;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

public class RwDistributionTrait implements RelDistribution {
  private static final Ordering<Iterable<Integer>> ORDERING =
      Ordering.<Integer>natural().lexicographical();

  private final Type type;
  private final ImmutableIntList keys;

  public RwDistributionTrait(Type type, ImmutableIntList keys) {
    checkArgs(type, keys);
    this.type = type;
    this.keys = sort(keys);
  }

  private static ImmutableIntList sort(ImmutableIntList keys) {
    var array = keys.toIntArray();
    Arrays.sort(array);
    return ImmutableIntList.of(array);
  }

  private static void checkArgs(Type type, ImmutableIntList keys) {
    requireNonNull(type, "type");
    requireNonNull(keys, "keys");
    if (Type.HASH_DISTRIBUTED == type) {
      verify(!keys.isEmpty(), "Hash distribution keys can't be empty!");
    } else if (Type.RANDOM_DISTRIBUTED != type) {
      verify(keys.isEmpty(), "Distribution key of type %s must be empty!", type);
    }
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public List<Integer> getKeys() {
    return keys;
  }

  @Override
  public RelDistribution apply(Mappings.TargetMapping mapping) {
    if (keys.isEmpty()) {
      return this;
    }
    for (int key : keys) {
      if (mapping.getTargetOpt(key) == -1) {
        return ANY; // Some distribution keys are not mapped => any.
      }
    }
    List<Integer> mappedKeys0 = Mappings.apply2((Mapping) mapping, keys);
    ImmutableIntList mappedKeys = normalizeKeys(mappedKeys0);
    return new RwDistributionTrait(type, mappedKeys);
  }

  @Override
  public boolean isTop() {
    return Type.ANY == type;
  }

  @Override
  public int compareTo(RelMultipleTrait o) {
    final RelDistribution distribution = (RelDistribution) o;
    if (type == distribution.getType()
        && (type == Type.HASH_DISTRIBUTED || type == Type.RANGE_DISTRIBUTED)) {
      return ORDERING.compare(getKeys(), distribution.getKeys());
    }

    return type.compareTo(distribution.getType());
  }

  @Override
  public RelTraitDef<RwDistributionTrait> getTraitDef() {
    return RwDistributionTraitDef.getInstance();
  }

  @Override
  public boolean satisfies(RelTrait trait) {
    if (trait == this || trait == RwDistributions.ANY) {
      return true;
    }
    if (trait instanceof RwDistributionTrait) {
      RwDistributionTrait other = (RwDistributionTrait) trait;
      if (type == other.type) {
        switch (type) {
          case HASH_DISTRIBUTED:
            // The "leading edge" property of Range does not apply to Hash.
            // Only Hash[x, y] satisfies Hash[x, y].
            return keys.equals(other.keys);
          case RANGE_DISTRIBUTED:
            // Range[x, y] satisfies Range[x, y, z] but not Range[x]
            return Util.startsWith(other.keys, keys);
          default:
            return true;
        }
      }

      if (other.type == Type.RANDOM_DISTRIBUTED && other.keys.isEmpty()) {
        // RANDOM is satisfied by HASH, ROUND-ROBIN, RANDOM, RANGE;
        // we've already checked RANDOM
        return type == Type.HASH_DISTRIBUTED
            || type == Type.ROUND_ROBIN_DISTRIBUTED
            || type == Type.RANGE_DISTRIBUTED;
      }
    }

    return false;
  }

  @Override
  public void register(RelOptPlanner planner) {}

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RwDistributionTrait that = (RwDistributionTrait) o;
    return type == that.type && Objects.equal(keys, that.keys);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(type, keys);
  }

  @Override
  public String toString() {
    return "RwDistributionTrait{" + "type=" + type + ", keys=" + keys + '}';
  }

  /** Creates ordered immutable copy of keys collection. */
  private static ImmutableIntList normalizeKeys(Collection<? extends Number> keys) {
    ImmutableIntList list = ImmutableIntList.copyOf(keys);
    if (list.size() > 1 && !Ordering.natural().isOrdered(list)) {
      list = ImmutableIntList.copyOf(Ordering.natural().sortedCopy(list));
    }
    return list;
  }
}
