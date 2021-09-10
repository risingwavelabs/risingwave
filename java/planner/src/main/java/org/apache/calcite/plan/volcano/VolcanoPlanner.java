/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.plan.volcano;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.plan.AbstractRelOptPlanner;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelDigest;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptMaterializations;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.Converter;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.metadata.CyclicMetadataException;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

import org.apiguardian.api.API;
import org.checkerframework.checker.nullness.qual.EnsuresNonNull;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;
import org.checkerframework.dataflow.qual.Pure;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

// We need this because this bug fix https://issues.apache.org/jira/browse/CALCITE-2166
// has been reverted in calcite 1.27.0, we should remove this when this is fixed.
/**
 * VolcanoPlanner optimizes queries by transforming expressions selectively
 * according to a dynamic programming algorithm.
 */
public class VolcanoPlanner extends AbstractRelOptPlanner {

  //~ Instance fields --------------------------------------------------------

  protected @MonotonicNonNull RelSubset root;

  /**
   * Operands that apply to a given class of {@link RelNode}.
   *
   * <p>Any operand can be an 'entry point' to a rule call, when a RelNode is
   * registered which matches the operand. This map allows us to narrow down
   * operands based on the class of the RelNode.</p>
   */
  private final Multimap<Class<? extends RelNode>, RelOptRuleOperand>
      classOperands = LinkedListMultimap.create();

  /**
   * List of all sets. Used only for debugging.
   */
  final List<RelSet> allSets = new ArrayList<>();

  /**
   * Canonical map from {@link String digest} to the unique
   * {@link RelNode relational expression} with that digest.
   */
  private final Map<RelDigest, RelNode> mapDigestToRel =
      new HashMap<>();

  /**
   * Map each registered expression ({@link RelNode}) to its equivalence set
   * ({@link RelSubset}).
   *
   * <p>We use an {@link IdentityHashMap} to simplify the process of merging
   * {@link RelSet} objects. Most {@link RelNode} objects are identified by
   * their digest, which involves the set that their child relational
   * expressions belong to. If those children belong to the same set, we have
   * to be careful, otherwise it gets incestuous.</p>
   */
  private final IdentityHashMap<RelNode, RelSubset> mapRel2Subset =
      new IdentityHashMap<>();

  /**
   * The nodes to be pruned.
   *
   * <p>If a RelNode is pruned, all {@link RelOptRuleCall}s using it
   * are ignored, and future RelOptRuleCalls are not queued up.
   */
  final Set<RelNode> prunedNodes = new HashSet<>();

  /**
   * List of all schemas which have been registered.
   */
  private final Set<RelOptSchema> registeredSchemas = new HashSet<>();

  /**
   * A driver to manage rule and rule matches.
   */
  RuleDriver ruleDriver;

  /**
   * Holds the currently registered RelTraitDefs.
   */
  private final List<RelTraitDef> traitDefs = new ArrayList<>();

  private int nextSetId = 0;

  private @MonotonicNonNull RelNode originalRoot;

  private @Nullable Convention rootConvention;

  /**
   * Whether the planner can accept new rules.
   */
  private boolean locked;

  /**
   * Whether rels with Convention.NONE has infinite cost.
   */
  private boolean noneConventionHasInfiniteCost = true;

  private final List<RelOptMaterialization> materializations =
      new ArrayList<>();

  /**
   * Map of lattices by the qualified name of their star table.
   */
  private final Map<List<String>, RelOptLattice> latticeByName =
      new LinkedHashMap<>();

  final Map<RelNode, Provenance> provenanceMap;

  final Deque<VolcanoRuleCall> ruleCallStack = new ArrayDeque<>();

  /** Zero cost, according to {@link #costFactory}. Not necessarily a
   * {@link org.apache.calcite.plan.volcano.VolcanoCost}. */
  final RelOptCost zeroCost;

  /** Infinite cost, according to {@link #costFactory}. Not necessarily a
   * {@link org.apache.calcite.plan.volcano.VolcanoCost}. */
  final RelOptCost infCost;

  /**
   * Whether to enable top-down optimization or not.
   */
  boolean topDownOpt = CalciteSystemProperty.TOPDOWN_OPT.value();

  /**
   * Extra roots for explorations.
   */
  Set<RelSubset> explorationRoots = new HashSet<>();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a uninitialized <code>VolcanoPlanner</code>. To fully initialize
   * it, the caller must register the desired set of relations, rules, and
   * calling conventions.
   */
  public VolcanoPlanner() {
    this(null, null);
  }

  /**
   * Creates a uninitialized <code>VolcanoPlanner</code>. To fully initialize
   * it, the caller must register the desired set of relations, rules, and
   * calling conventions.
   */
  public VolcanoPlanner(Context externalContext) {
    this(null, externalContext);
  }

  /**
   * Creates a {@code VolcanoPlanner} with a given cost factory.
   */
  @SuppressWarnings("method.invocation.invalid")
  public VolcanoPlanner(@Nullable RelOptCostFactory costFactory,
                        @Nullable Context externalContext) {
    super(costFactory == null ? VolcanoCost.FACTORY : costFactory,
        externalContext);
    this.zeroCost = this.costFactory.makeZeroCost();
    this.infCost = this.costFactory.makeInfiniteCost();
    // If LOGGER is debug enabled, enable provenance information to be captured
    this.provenanceMap = LOGGER.isDebugEnabled() ? new HashMap<>()
        : Util.blackholeMap();
    initRuleQueue();
  }

  @EnsuresNonNull("ruleDriver")
  private void initRuleQueue() {
    if (topDownOpt) {
      ruleDriver = new TopDownRuleDriver(this);
    } else {
      ruleDriver = new IterativeRuleDriver(this);
    }
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Enable or disable top-down optimization.
   *
   * <p>Note: Enabling top-down optimization will automatically enable
   * top-down trait propagation.</p>
   */
  public void setTopDownOpt(boolean value) {
    if (topDownOpt == value) {
      return;
    }
    topDownOpt = value;
    initRuleQueue();
  }

  // implement RelOptPlanner
  @Override public boolean isRegistered(RelNode rel) {
    return mapRel2Subset.get(rel) != null;
  }

  @Override public void setRoot(RelNode rel) {
    // We've registered all the rules, and therefore RelNode classes,
    // we're interested in, and have not yet started calling metadata providers.
    // So now is a good time to tell the metadata layer what to expect.
    registerMetadataRels();

    this.root = registerImpl(rel, null);
    if (this.originalRoot == null) {
      this.originalRoot = rel;
    }

    rootConvention = this.root.getConvention();
    ensureRootConverters();
  }

  @Pure
  @Override public @Nullable RelNode getRoot() {
    return root;
  }

  @Override public List<RelOptMaterialization> getMaterializations() {
    return ImmutableList.copyOf(materializations);
  }

  @Override public void addMaterialization(
      RelOptMaterialization materialization) {
    materializations.add(materialization);
  }

  @Override public void addLattice(RelOptLattice lattice) {
    latticeByName.put(lattice.starRelOptTable.getQualifiedName(), lattice);
  }

  @Override public @Nullable RelOptLattice getLattice(RelOptTable table) {
    return latticeByName.get(table.getQualifiedName());
  }

  protected void registerMaterializations() {
    // Avoid using materializations while populating materializations!
    final CalciteConnectionConfig config =
        context.unwrap(CalciteConnectionConfig.class);
    if (config == null || !config.materializationsEnabled()) {
      return;
    }

    assert root != null : "root";
    assert originalRoot != null : "originalRoot";

    // Register rels using materialized views.
    final List<Pair<RelNode, List<RelOptMaterialization>>> materializationUses =
        RelOptMaterializations.useMaterializedViews(originalRoot, materializations);
    for (Pair<RelNode, List<RelOptMaterialization>> use : materializationUses) {
      RelNode rel = use.left;
      Hook.SUB.run(rel);
      registerImpl(rel, root.set);
    }

    // Register table rels of materialized views that cannot find a substitution
    // in root rel transformation but can potentially be useful.
    final Set<RelOptMaterialization> applicableMaterializations =
        new HashSet<>(
            RelOptMaterializations.getApplicableMaterializations(
                originalRoot, materializations));
    for (Pair<RelNode, List<RelOptMaterialization>> use : materializationUses) {
      applicableMaterializations.removeAll(use.right);
    }
    for (RelOptMaterialization materialization : applicableMaterializations) {
      RelSubset subset = registerImpl(materialization.queryRel, null);
      explorationRoots.add(subset);
      RelNode tableRel2 =
          RelOptUtil.createCastRel(
              materialization.tableRel,
              materialization.queryRel.getRowType(),
              true);
      registerImpl(tableRel2, subset.set);
    }

    // Register rels using lattices.
    final List<Pair<RelNode, RelOptLattice>> latticeUses =
        RelOptMaterializations.useLattices(
            originalRoot, ImmutableList.copyOf(latticeByName.values()));
    if (!latticeUses.isEmpty()) {
      RelNode rel = latticeUses.get(0).left;
      Hook.SUB.run(rel);
      registerImpl(rel, root.set);
    }
  }

  /**
   * Finds an expression's equivalence set. If the expression is not
   * registered, returns null.
   *
   * @param rel Relational expression
   * @return Equivalence set that expression belongs to, or null if it is not
   * registered
   */
  public @Nullable RelSet getSet(RelNode rel) {
    assert rel != null : "pre: rel != null";
    final RelSubset subset = getSubset(rel);
    if (subset != null) {
      assert subset.set != null;
      return subset.set;
    }
    return null;
  }

  @Override public boolean addRelTraitDef(RelTraitDef relTraitDef) {
    return !traitDefs.contains(relTraitDef) && traitDefs.add(relTraitDef);
  }

  @Override public void clearRelTraitDefs() {
    traitDefs.clear();
  }

  @Override public List<RelTraitDef> getRelTraitDefs() {
    return traitDefs;
  }

  @Override public RelTraitSet emptyTraitSet() {
    RelTraitSet traitSet = super.emptyTraitSet();
    for (RelTraitDef traitDef : traitDefs) {
      if (traitDef.multiple()) {
        // TODO: restructure RelTraitSet to allow a list of entries
        //  for any given trait
      }
      traitSet = traitSet.plus(traitDef.getDefault());
    }
    return traitSet;
  }

  @Override public void clear() {
    super.clear();
    for (RelOptRule rule : getRules()) {
      removeRule(rule);
    }
    this.classOperands.clear();
    this.allSets.clear();
    this.mapDigestToRel.clear();
    this.mapRel2Subset.clear();
    this.prunedNodes.clear();
    this.ruleDriver.clear();
    this.materializations.clear();
    this.latticeByName.clear();
    this.provenanceMap.clear();
  }

  @Override public boolean addRule(RelOptRule rule) {
    if (locked) {
      return false;
    }

    if (!super.addRule(rule)) {
      return false;
    }

    // Each of this rule's operands is an 'entry point' for a rule call.
    // Register each operand against all concrete sub-classes that could match
    // it.
    for (RelOptRuleOperand operand : rule.getOperands()) {
      for (Class<? extends RelNode> subClass
          : subClasses(operand.getMatchedClass())) {
        if (PhysicalNode.class.isAssignableFrom(subClass)
            && rule instanceof TransformationRule) {
          continue;
        }
        classOperands.put(subClass, operand);
      }
    }

    // If this is a converter rule, check that it operates on one of the
    // kinds of trait we are interested in, and if so, register the rule
    // with the trait.
    if (rule instanceof ConverterRule) {
      ConverterRule converterRule = (ConverterRule) rule;

      final RelTrait ruleTrait = converterRule.getInTrait();
      final RelTraitDef ruleTraitDef = ruleTrait.getTraitDef();
      if (traitDefs.contains(ruleTraitDef)) {
        ruleTraitDef.registerConverterRule(this, converterRule);
      }
    }

    return true;
  }

  @Override public boolean removeRule(RelOptRule rule) {
    // Remove description.
    if (!super.removeRule(rule)) {
      // Rule was not present.
      return false;
    }

    // Remove operands.
    classOperands.values().removeIf(entry -> entry.getRule().equals(rule));

    // Remove trait mappings. (In particular, entries from conversion
    // graph.)
    if (rule instanceof ConverterRule) {
      ConverterRule converterRule = (ConverterRule) rule;
      final RelTrait ruleTrait = converterRule.getInTrait();
      final RelTraitDef ruleTraitDef = ruleTrait.getTraitDef();
      if (traitDefs.contains(ruleTraitDef)) {
        ruleTraitDef.deregisterConverterRule(this, converterRule);
      }
    }
    return true;
  }

  @Override protected void onNewClass(RelNode node) {
    super.onNewClass(node);

    final boolean isPhysical = node instanceof PhysicalNode;
    // Create mappings so that instances of this class will match existing
    // operands.
    final Class<? extends RelNode> clazz = node.getClass();
    for (RelOptRule rule : mapDescToRule.values()) {
      if (isPhysical && rule instanceof TransformationRule) {
        continue;
      }
      for (RelOptRuleOperand operand : rule.getOperands()) {
        if (operand.getMatchedClass().isAssignableFrom(clazz)) {
          classOperands.put(clazz, operand);
        }
      }
    }
  }

  @Override public RelNode changeTraits(final RelNode rel, RelTraitSet toTraits) {
    assert !rel.getTraitSet().equals(toTraits);
    assert toTraits.allSimple();

    RelSubset rel2 = ensureRegistered(rel, null);
    if (rel2.getTraitSet().equals(toTraits)) {
      return rel2;
    }

    return rel2.set.getOrCreateSubset(
        rel.getCluster(), toTraits, true);
  }

  @Override public RelOptPlanner chooseDelegate() {
    return this;
  }

  /**
   * Finds the most efficient expression to implement the query given via
   * {@link org.apache.calcite.plan.RelOptPlanner#setRoot(org.apache.calcite.rel.RelNode)}.
   *
   * @return the most efficient RelNode tree found for implementing the given
   * query
   */
  @Override public RelNode findBestExp() {
    assert root != null : "root must not be null";
    ensureRootConverters();
    registerMaterializations();

    ruleDriver.drive();

    if (LOGGER.isTraceEnabled()) {
      StringWriter sw = new StringWriter();
      final PrintWriter pw = new PrintWriter(sw);
      dump(pw);
      pw.flush();
      LOGGER.info(sw.toString());
    }
    dumpRuleAttemptsInfo();
    RelNode cheapest = root.buildCheapestPlan(this);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Cheapest plan:\n{}", RelOptUtil.toString(cheapest, SqlExplainLevel.ALL_ATTRIBUTES));

      if (!provenanceMap.isEmpty()) {
        LOGGER.debug("Provenance:\n{}", Dumpers.provenance(provenanceMap, cheapest));
      }
    }
    return cheapest;
  }

  @Override public void checkCancel() {
    if (cancelFlag.get()) {
      throw new VolcanoTimeoutException();
    }
  }

  /** Informs {@link JaninoRelMetadataProvider} about the different kinds of
   * {@link RelNode} that we will be dealing with. It will reduce the number
   * of times that we need to re-generate the provider. */
  private void registerMetadataRels() {
    JaninoRelMetadataProvider.DEFAULT.register(classOperands.keySet());
  }

  /** Ensures that the subset that is the root relational expression contains
   * converters to all other subsets in its equivalence set.
   *
   * <p>Thus the planner tries to find cheap implementations of those other
   * subsets, which can then be converted to the root. This is the only place
   * in the plan where explicit converters are required; elsewhere, a consumer
   * will be asking for the result in a particular convention, but the root has
   * no consumers. */
  @RequiresNonNull("root")
  void ensureRootConverters() {
    final Set<RelSubset> subsets = new HashSet<>();
    for (RelNode rel : root.getRels()) {
      if (rel instanceof AbstractConverter) {
        subsets.add((RelSubset) ((AbstractConverter) rel).getInput());
      }
    }
    for (RelSubset subset : root.set.subsets) {
      final ImmutableList<RelTrait> difference =
          root.getTraitSet().difference(subset.getTraitSet());
      if (difference.size() == 1 && subsets.add(subset)) {
        register(
            new AbstractConverter(subset.getCluster(), subset,
                difference.get(0).getTraitDef(), root.getTraitSet()),
            root);
      }
    }
  }

  @Override public RelSubset register(
      RelNode rel,
      @Nullable RelNode equivRel) {
    assert !isRegistered(rel) : "pre: isRegistered(rel)";
    final RelSet set;
    if (equivRel == null) {
      set = null;
    } else {
      final RelDataType relType = rel.getRowType();
      final RelDataType equivRelType = equivRel.getRowType();
      if (!RelOptUtil.areRowTypesEqual(relType,
          equivRelType, false)) {
        throw new IllegalArgumentException(
            RelOptUtil.getFullTypeDifferenceString("rel rowtype", relType,
                "equiv rowtype", equivRelType));
      }
      equivRel = ensureRegistered(equivRel, null);
      set = getSet(equivRel);
    }
    return registerImpl(rel, set);
  }

  @Override public RelSubset ensureRegistered(RelNode rel, @Nullable RelNode equivRel) {
    RelSubset result;
    final RelSubset subset = getSubset(rel);
    if (subset != null) {
      if (equivRel != null) {
        final RelSubset equivSubset = getSubsetNonNull(equivRel);
        if (subset.set != equivSubset.set) {
          merge(equivSubset.set, subset.set);
        }
      }
      result = canonize(subset);
    } else {
      result = register(rel, equivRel);
    }

    // Checking if tree is valid considerably slows down planning
    // Only doing it if logger level is debug or finer
    if (LOGGER.isDebugEnabled()) {
      assert isValid(Litmus.THROW);
    }

    return result;
  }

  /**
   * Checks internal consistency.
   */
  protected boolean isValid(Litmus litmus) {
    RelNode root = getRoot();
    if (root == null) {
      return true;
    }

    RelMetadataQuery metaQuery = root.getCluster().getMetadataQuerySupplier().get();
    for (RelSet set : allSets) {
      if (set.equivalentSet != null) {
        return litmus.fail("set [{}] has been merged: it should not be in the list", set);
      }
      for (RelSubset subset : set.subsets) {
        if (subset.set != set) {
          return litmus.fail("subset [{}] is in wrong set [{}]",
              subset, set);
        }

        if (subset.best != null) {

          // Make sure best RelNode is valid
          if (!subset.set.rels.contains(subset.best)) {
            return litmus.fail("RelSubset [{}] does not contain its best RelNode [{}]",
                subset, subset.best);
          }

          // Make sure bestCost is up-to-date
          try {
            RelOptCost bestCost = getCostOrInfinite(subset.best, metaQuery);
            if (!subset.bestCost.equals(bestCost)) {
              return litmus.fail("RelSubset [" + subset
                  + "] has wrong best cost "
                  + subset.bestCost + ". Correct cost is " + bestCost);
            }
          } catch (CyclicMetadataException e) {
            // ignore
          }
        }

        for (RelNode rel : subset.getRels()) {
          try {
            RelOptCost relCost = getCost(rel, metaQuery);
            if (relCost != null && relCost.isLt(subset.bestCost)) {
              return litmus.fail("rel [{}] has lower cost {} than "
                      + "best cost {} of subset [{}]",
                  rel, relCost, subset.bestCost, subset);
            }
          } catch (CyclicMetadataException e) {
            // ignore
          }
        }
      }
    }
    return litmus.succeed();
  }

  public void registerAbstractRelationalRules() {
    RelOptUtil.registerAbstractRelationalRules(this);
  }

  @Override public void registerSchema(RelOptSchema schema) {
    if (registeredSchemas.add(schema)) {
      try {
        schema.registerRules(this);
      } catch (Exception e) {
        throw new AssertionError("While registering schema " + schema, e);
      }
    }
  }

  /**
   * Sets whether this planner should consider rel nodes with Convention.NONE
   * to have infinite cost or not.
   * @param infinite Whether to make none convention rel nodes infinite cost
   */
  public void setNoneConventionHasInfiniteCost(boolean infinite) {
    this.noneConventionHasInfiniteCost = infinite;
  }

  /**
   * Returns cost of a relation or infinite cost if the cost is not known.
   * @param rel relation t
   * @param mq metadata query
   * @return cost of the relation or infinite cost if the cost is not known
   * @see org.apache.calcite.plan.volcano.RelSubset#bestCost
   */
  private RelOptCost getCostOrInfinite(RelNode rel, RelMetadataQuery mq) {
    RelOptCost cost = getCost(rel, mq);
    return cost == null ? infCost : cost;
  }

  @Override public @Nullable RelOptCost getCost(RelNode rel, RelMetadataQuery mq) {
    assert rel != null : "pre-condition: rel != null";
    if (rel instanceof RelSubset) {
      return ((RelSubset) rel).bestCost;
    }
    if (noneConventionHasInfiniteCost
        && rel.getTraitSet().getTrait(ConventionTraitDef.INSTANCE) == Convention.NONE) {
      return costFactory.makeInfiniteCost();
    }
    RelOptCost cost = mq.getNonCumulativeCost(rel);
    if (cost == null) {
      return null;
    }
    if (!zeroCost.isLt(cost)) {
      // cost must be positive, so nudge it
      cost = costFactory.makeTinyCost();
    }
    for (RelNode input : rel.getInputs()) {
      RelOptCost inputCost = getCost(input, mq);
      if (inputCost == null) {
        return null;
      }
      cost = cost.plus(inputCost);
    }
    return cost;
  }

  /**
   * Returns the subset that a relational expression belongs to.
   *
   * @param rel Relational expression
   * @return Subset it belongs to, or null if it is not registered
   */
  public @Nullable RelSubset getSubset(RelNode rel) {
    assert rel != null : "pre: rel != null";
    if (rel instanceof RelSubset) {
      return (RelSubset) rel;
    } else {
      return mapRel2Subset.get(rel);
    }
  }

  /**
   * Returns the subset that a relational expression belongs to.
   *
   * @param rel Relational expression
   * @return Subset it belongs to, or null if it is not registered
   * @throws AssertionError in case subset is not found
   */
  @API(since = "1.26", status = API.Status.EXPERIMENTAL)
  public RelSubset getSubsetNonNull(RelNode rel) {
    return requireNonNull(getSubset(rel), () -> "Subset is not found for " + rel);
  }

  public @Nullable RelSubset getSubset(RelNode rel, RelTraitSet traits) {
    if ((rel instanceof RelSubset) && rel.getTraitSet().equals(traits)) {
      return (RelSubset) rel;
    }
    RelSet set = getSet(rel);
    if (set == null) {
      return null;
    }
    return set.getSubset(traits);
  }

  @Nullable RelNode changeTraitsUsingConverters(
      RelNode rel,
      RelTraitSet toTraits) {
    final RelTraitSet fromTraits = rel.getTraitSet();

    assert fromTraits.size() >= toTraits.size();

    final boolean allowInfiniteCostConverters =
        CalciteSystemProperty.ALLOW_INFINITE_COST_CONVERTERS.value();

    // Traits may build on top of another...for example a collation trait
    // would typically come after a distribution trait since distribution
    // destroys collation; so when doing the conversion below we use
    // fromTraits as the trait of the just previously converted RelNode.
    // Also, toTraits may have fewer traits than fromTraits, excess traits
    // will be left as is.  Finally, any null entries in toTraits are
    // ignored.
    RelNode converted = rel;
    for (int i = 0; (converted != null) && (i < toTraits.size()); i++) {
      RelTrait fromTrait = converted.getTraitSet().getTrait(i);
      final RelTraitDef traitDef = fromTrait.getTraitDef();
      RelTrait toTrait = toTraits.getTrait(i);

      if (toTrait == null) {
        continue;
      }

      assert traitDef == toTrait.getTraitDef();
      if (fromTrait.satisfies(toTrait)) {
        // No need to convert; it's already correct.
        continue;
      }

      RelNode convertedRel =
          traitDef.convert(
              this,
              converted,
              toTrait,
              allowInfiniteCostConverters);
      if (convertedRel != null) {
        assert castNonNull(convertedRel.getTraitSet().getTrait(traitDef)).satisfies(toTrait);
        register(convertedRel, converted);
      }

      converted = convertedRel;
    }

    // make sure final converted traitset subsumes what was required
    if (converted != null) {
      assert converted.getTraitSet().satisfies(toTraits);
    }

    return converted;
  }

  @Override public void prune(RelNode rel) {
    prunedNodes.add(rel);
  }

  /**
   * Dumps the internal state of this VolcanoPlanner to a writer.
   *
   * @param pw Print writer
   * @see #normalizePlan(String)
   */
  public void dump(PrintWriter pw) {
    pw.println("Root: " + root);
    pw.println("Original rel:");

    if (originalRoot != null) {
      originalRoot.explain(
          new RelWriterImpl(pw, SqlExplainLevel.ALL_ATTRIBUTES, false));
    }

    try {
      if (CalciteSystemProperty.DUMP_SETS.value()) {
        pw.println();
        pw.println("Sets:");
        Dumpers.dumpSets(this, pw);
      }
      if (CalciteSystemProperty.DUMP_GRAPHVIZ.value()) {
        pw.println();
        pw.println("Graphviz:");
        Dumpers.dumpGraphviz(this, pw);
      }
    } catch (Exception | AssertionError e) {
      pw.println("Error when dumping plan state: \n"
          + e);
    }
  }

  public String toDot() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    Dumpers.dumpGraphviz(this, pw);
    pw.flush();
    return sw.toString();
  }

  /**
   * Re-computes the digest of a {@link RelNode}.
   *
   * <p>Since a relational expression's digest contains the identifiers of its
   * children, this method needs to be called when the child has been renamed,
   * for example if the child's set merges with another.
   *
   * @param rel Relational expression
   */
  void rename(RelNode rel) {
    String oldDigest = "";
    if (LOGGER.isTraceEnabled()) {
      oldDigest = rel.getDigest();
    }
    if (fixUpInputs(rel)) {
      final RelDigest newDigest = rel.getRelDigest();
      LOGGER.trace("Rename #{} from '{}' to '{}'", rel.getId(), oldDigest, newDigest);
      final RelNode equivRel = mapDigestToRel.put(newDigest, rel);
      if (equivRel != null) {
        assert equivRel != rel;

        // There's already an equivalent with the same name, and we
        // just knocked it out. Put it back, and forget about 'rel'.
        LOGGER.trace("After renaming rel#{} it is now equivalent to rel#{}",
            rel.getId(), equivRel.getId());

        mapDigestToRel.put(newDigest, equivRel);
        checkPruned(equivRel, rel);

        RelSubset equivRelSubset = getSubsetNonNull(equivRel);

        // Remove back-links from children.
        for (RelNode input : rel.getInputs()) {
          ((RelSubset) input).set.parents.remove(rel);
        }

        // Remove rel from its subset. (This may leave the subset
        // empty, but if so, that will be dealt with when the sets
        // get merged.)
        final RelSubset subset = mapRel2Subset.put(rel, equivRelSubset);
        assert subset != null;
        boolean existed = subset.set.rels.remove(rel);
        assert existed : "rel was not known to its set";
        final RelSubset equivSubset = getSubsetNonNull(equivRel);
        for (RelSubset s : subset.set.subsets) {
          if (s.best == rel) {
            s.best = equivRel;
            // Propagate cost improvement since this potentially would change the subset's best cost
            propagateCostImprovements(equivRel);
          }
        }

        if (equivSubset != subset) {
          // The equivalent relational expression is in a different
          // subset, therefore the sets are equivalent.
          assert equivSubset.getTraitSet().equals(
              subset.getTraitSet());
          assert equivSubset.set != subset.set;
          merge(equivSubset.set, subset.set);
        }
      }
    }
  }

  /**
   * Checks whether a relexp has made any subset cheaper, and if it so,
   * propagate new cost to parent rel nodes.
   *
   * @param rel       Relational expression whose cost has improved
   */
  void propagateCostImprovements(RelNode rel) {
    RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    Map<RelNode, RelOptCost> propagateRels = new HashMap<>();
    PriorityQueue<RelNode> propagateHeap = new PriorityQueue<>((o1, o2) -> {
      RelOptCost c1 = propagateRels.get(o1);
      RelOptCost c2 = propagateRels.get(o2);
      if (c1 == null) {
        return c2 == null ? 0 : -1;
      }
      if (c2 == null) {
        return 1;
      }
      if (c1.equals(c2)) {
        return 0;
      } else if (c1.isLt(c2)) {
        return -1;
      }
      return 1;
    });
    propagateRels.put(rel, getCostOrInfinite(rel, mq));
    propagateHeap.offer(rel);

    RelNode relNode;
    while ((relNode = propagateHeap.poll()) != null) {
      RelOptCost cost = requireNonNull(propagateRels.get(relNode), "propagateRels.get(relNode)");

      for (RelSubset subset : getSubsetNonNull(relNode).set.subsets) {
        if (!relNode.getTraitSet().satisfies(subset.getTraitSet())) {
          continue;
        }

        // BEGIN CHANGE OF RISINGWAVE
        if ((relNode != subset.best) && !cost.isLt(subset.bestCost)) {
          continue;
        }
        // END CHANGE OF RISINGWAVE

        // Update subset best cost when we find a cheaper rel or the current
        // best's cost is changed
        subset.timestamp++;
        LOGGER.trace("Subset cost changed: subset [{}] cost was {} now {}",
            subset, subset.bestCost, cost);

        subset.bestCost = cost;
        subset.best = relNode;
        // since best was changed, cached metadata for this subset should be removed
        mq.clearCache(subset);

        for (RelNode parent : subset.getParents()) {
          mq.clearCache(parent);
          RelOptCost newCost = getCostOrInfinite(parent, mq);
          RelOptCost existingCost = propagateRels.get(parent);
          if (existingCost == null || newCost.isLt(existingCost)) {
            propagateRels.put(parent, newCost);
            if (existingCost != null) {
              // Cost reduced, force the heap to adjust its ordering
              propagateHeap.remove(parent);
            }
            propagateHeap.offer(parent);
          }
        }
      }
    }
  }

  /**
   * Registers a {@link RelNode}, which has already been registered, in a new
   * {@link RelSet}.
   *
   * @param set Set
   * @param rel Relational expression
   */
  void reregister(
      RelSet set,
      RelNode rel) {
    // Is there an equivalent relational expression? (This might have
    // just occurred because the relational expression's child was just
    // found to be equivalent to another set.)
    RelNode equivRel = mapDigestToRel.get(rel.getRelDigest());
    if (equivRel != null && equivRel != rel) {
      assert equivRel.getClass() == rel.getClass();
      assert equivRel.getTraitSet().equals(rel.getTraitSet());

      checkPruned(equivRel, rel);
      return;
    }

    // Add the relational expression into the correct set and subset.
    if (!prunedNodes.contains(rel)) {
      addRelToSet(rel, set);
    }
  }

  /**
   * Prune rel node if the latter one (identical with rel node)
   * is already pruned.
   */
  private void checkPruned(RelNode rel, RelNode duplicateRel) {
    if (prunedNodes.contains(duplicateRel)) {
      prunedNodes.add(rel);
    }
  }

  /**
   * Find the new root subset in case the root is merged with another subset.
   */
  @RequiresNonNull("root")
  void canonize() {
    root = canonize(root);
  }

  /**
   * If a subset has one or more equivalent subsets (owing to a set having
   * merged with another), returns the subset which is the leader of the
   * equivalence class.
   *
   * @param subset Subset
   * @return Leader of subset's equivalence class
   */
  private static RelSubset canonize(final RelSubset subset) {
    RelSet set = subset.set;
    if (set.equivalentSet == null) {
      return subset;
    }
    do {
      set = set.equivalentSet;
    } while (set.equivalentSet != null);
    return set.getOrCreateSubset(
        subset.getCluster(), subset.getTraitSet(), subset.isRequired());
  }

  /**
   * Fires all rules matched by a relational expression.
   *
   * @param rel      Relational expression which has just been created (or maybe
   *                 from the queue)
   */
  void fireRules(RelNode rel) {
    for (RelOptRuleOperand operand : classOperands.get(rel.getClass())) {
      if (operand.matches(rel)) {
        final VolcanoRuleCall ruleCall;
        ruleCall = new DeferringRuleCall(this, operand);
        ruleCall.match(rel);
      }
    }
  }

  private boolean fixUpInputs(RelNode rel) {
    List<RelNode> inputs = rel.getInputs();
    List<RelNode> newInputs = new ArrayList<>(inputs.size());
    int changeCount = 0;
    for (RelNode input : inputs) {
      assert input instanceof RelSubset;
      final RelSubset subset = (RelSubset) input;
      RelSubset newSubset = canonize(subset);
      newInputs.add(newSubset);
      if (newSubset != subset) {
        if (subset.set != newSubset.set) {
          subset.set.parents.remove(rel);
          newSubset.set.parents.add(rel);
        }
        changeCount++;
      }
    }

    if (changeCount > 0) {
      RelMdUtil.clearCache(rel);
      RelNode removed = mapDigestToRel.remove(rel.getRelDigest());
      assert removed == rel;
      for (int i = 0; i < inputs.size(); i++) {
        rel.replaceInput(i, newInputs.get(i));
      }
      rel.recomputeDigest();
      return true;
    }
    return false;
  }

  private RelSet merge(RelSet set1, RelSet set2) {
    assert set1 != set2 : "pre: set1 != set2";

    // Find the root of each set's equivalence tree.
    set1 = equivRoot(set1);
    set2 = equivRoot(set2);

    // If set1 and set2 are equivalent, there's nothing to do.
    if (set2 == set1) {
      return set1;
    }

    // If necessary, swap the sets, so we're always merging the newer set
    // into the older or merging parent set into child set.
    final boolean swap;
    final Set<RelSet> childrenOf1 = set1.getChildSets(this);
    final Set<RelSet> childrenOf2 = set2.getChildSets(this);
    final boolean set2IsParentOfSet1 = childrenOf2.contains(set1);
    final boolean set1IsParentOfSet2 = childrenOf1.contains(set2);
    if (set2IsParentOfSet1 && set1IsParentOfSet2) {
      // There is a cycle of length 1; each set is the (direct) parent of the
      // other. Swap so that we are merging into the larger, older set.
      swap = isSmaller(set1, set2);
    } else if (set2IsParentOfSet1) {
      // set2 is a parent of set1. Do not swap. We want to merge set2 into set.
      swap = false;
    } else if (set1IsParentOfSet2) {
      // set1 is a parent of set2. Swap, so that we merge set into set2.
      swap = true;
    } else {
      // Neither is a parent of the other.
      // Swap so that we are merging into the larger, older set.
      swap = isSmaller(set1, set2);
    }
    if (swap) {
      RelSet t = set1;
      set1 = set2;
      set2 = t;
    }

    // Merge.
    set1.mergeWith(this, set2);

    if (root == null) {
      throw new IllegalStateException("root must not be null");
    }

    // Was the set we merged with the root? If so, the result is the new
    // root.
    if (set2 == getSet(root)) {
      root = set1.getOrCreateSubset(
          root.getCluster(), root.getTraitSet(), root.isRequired());
      ensureRootConverters();
    }

    if (ruleDriver != null) {
      ruleDriver.onSetMerged(set1);
    }
    return set1;
  }

  /** Returns whether {@code set1} is less popular than {@code set2}
   * (or smaller, or younger). If so, it will be more efficient to merge set1
   * into set2 than set2 into set1. */
  private static boolean isSmaller(RelSet set1, RelSet set2) {
    if (set1.parents.size() != set2.parents.size()) {
      return set1.parents.size() < set2.parents.size(); // true if set1 is less popular than set2
    }
    if (set1.rels.size() != set2.rels.size()) {
      return set1.rels.size() < set2.rels.size(); // true if set1 is smaller than set2
    }
    return set1.id > set2.id; // true if set1 is younger than set2
  }

  static RelSet equivRoot(RelSet s) {
    RelSet p = s; // iterates at twice the rate, to detect cycles
    while (s.equivalentSet != null) {
      p = forward2(s, p);
      s = s.equivalentSet;
    }
    return s;
  }

  /** Moves forward two links, checking for a cycle at each. */
  private static @Nullable RelSet forward2(RelSet s, @Nullable RelSet p) {
    p = forward1(s, p);
    p = forward1(s, p);
    return p;
  }

  /** Moves forward one link, checking for a cycle. */
  private static @Nullable RelSet forward1(RelSet s, @Nullable RelSet p) {
    if (p != null) {
      p = p.equivalentSet;
      if (p == s) {
        throw new AssertionError("cycle in equivalence tree");
      }
    }
    return p;
  }

  /**
   * Registers a new expression <code>exp</code> and queues up rule matches.
   * If <code>set</code> is not null, makes the expression part of that
   * equivalence set. If an identical expression is already registered, we
   * don't need to register this one and nor should we queue up rule matches.
   *
   * @param rel relational expression to register. Must be either a
   *         {@link RelSubset}, or an unregistered {@link RelNode}
   * @param set set that rel belongs to, or <code>null</code>
   * @return the equivalence-set
   */
  private RelSubset registerImpl(
      RelNode rel,
      @Nullable RelSet set) {
    if (rel instanceof RelSubset) {
      return registerSubset(set, (RelSubset) rel);
    }

    assert !isRegistered(rel) : "already been registered: " + rel;
    if (rel.getCluster().getPlanner() != this) {
      throw new AssertionError("Relational expression " + rel
          + " belongs to a different planner than is currently being used.");
    }

    // Now is a good time to ensure that the relational expression
    // implements the interface required by its calling convention.
    final RelTraitSet traits = rel.getTraitSet();
    final Convention convention = traits.getTrait(ConventionTraitDef.INSTANCE);
    assert convention != null;
    if (!convention.getInterface().isInstance(rel)
        && !(rel instanceof Converter)) {
      throw new AssertionError("Relational expression " + rel
          + " has calling-convention " + convention
          + " but does not implement the required interface '"
          + convention.getInterface() + "' of that convention");
    }
    if (traits.size() != traitDefs.size()) {
      throw new AssertionError("Relational expression " + rel
          + " does not have the correct number of traits: " + traits.size()
          + " != " + traitDefs.size());
    }

    // Ensure that its sub-expressions are registered.
    rel = rel.onRegister(this);

    // Record its provenance. (Rule call may be null.)
    final VolcanoRuleCall ruleCall = ruleCallStack.peek();
    if (ruleCall == null) {
      provenanceMap.put(rel, Provenance.EMPTY);
    } else {
      provenanceMap.put(
          rel,
          new RuleProvenance(
              ruleCall.rule,
              ImmutableList.copyOf(ruleCall.rels),
              ruleCall.id));
    }

    // If it is equivalent to an existing expression, return the set that
    // the equivalent expression belongs to.
    RelDigest digest = rel.getRelDigest();
    RelNode equivExp = mapDigestToRel.get(digest);
    if (equivExp == null) {
      // do nothing
    } else if (equivExp == rel) {
      // The same rel is already registered, so return its subset
      return getSubsetNonNull(equivExp);
    } else {
      if (!RelOptUtil.areRowTypesEqual(equivExp.getRowType(),
          rel.getRowType(), false)) {
        throw new IllegalArgumentException(
            RelOptUtil.getFullTypeDifferenceString("equiv rowtype",
                equivExp.getRowType(), "rel rowtype", rel.getRowType()));
      }
      checkPruned(equivExp, rel);

      RelSet equivSet = getSet(equivExp);
      if (equivSet != null) {
        LOGGER.trace(
            "Register: rel#{} is equivalent to {}", rel.getId(), equivExp);
        return registerSubset(set, getSubsetNonNull(equivExp));
      }
    }

    // Converters are in the same set as their children.
    if (rel instanceof Converter) {
      final RelNode input = ((Converter) rel).getInput();
      final RelSet childSet = castNonNull(getSet(input));
      if ((set != null)
          && (set != childSet)
          && (set.equivalentSet == null)) {
        LOGGER.trace(
            "Register #{} {} (and merge sets, because it is a conversion)",
            rel.getId(), rel.getRelDigest());
        merge(set, childSet);

        // During the mergers, the child set may have changed, and since
        // we're not registered yet, we won't have been informed. So
        // check whether we are now equivalent to an existing
        // expression.
        if (fixUpInputs(rel)) {
          digest = rel.getRelDigest();
          RelNode equivRel = mapDigestToRel.get(digest);
          if ((equivRel != rel) && (equivRel != null)) {

            // make sure this bad rel didn't get into the
            // set in any way (fixupInputs will do this but it
            // doesn't know if it should so it does it anyway)
            set.obliterateRelNode(rel);

            // There is already an equivalent expression. Use that
            // one, and forget about this one.
            return getSubsetNonNull(equivRel);
          }
        }
      } else {
        set = childSet;
      }
    }

    // Place the expression in the appropriate equivalence set.
    if (set == null) {
      set = new RelSet(
          nextSetId++,
          Util.minus(
              RelOptUtil.getVariablesSet(rel),
              rel.getVariablesSet()),
          RelOptUtil.getVariablesUsed(rel));
      this.allSets.add(set);
    }

    // Chain to find 'live' equivalent set, just in case several sets are
    // merging at the same time.
    while (set.equivalentSet != null) {
      set = set.equivalentSet;
    }

    // Allow each rel to register its own rules.
    registerClass(rel);

    final int subsetBeforeCount = set.subsets.size();
    RelSubset subset = addRelToSet(rel, set);

    final RelNode xx = mapDigestToRel.putIfAbsent(digest, rel);

    LOGGER.trace("Register {} in {}", rel, subset);

    // This relational expression may have been registered while we
    // recursively registered its children. If this is the case, we're done.
    if (xx != null) {
      return subset;
    }

    for (RelNode input : rel.getInputs()) {
      RelSubset childSubset = (RelSubset) input;
      childSubset.set.parents.add(rel);
    }

    // Queue up all rules triggered by this relexp's creation.
    fireRules(rel);

    // It's a new subset.
    if (set.subsets.size() > subsetBeforeCount
        || subset.triggerRule) {
      fireRules(subset);
    }

    return subset;
  }

  private RelSubset addRelToSet(RelNode rel, RelSet set) {
    RelSubset subset = set.add(rel);
    mapRel2Subset.put(rel, subset);

    // While a tree of RelNodes is being registered, sometimes nodes' costs
    // improve and the subset doesn't hear about it. You can end up with
    // a subset with a single rel of cost 99 which thinks its best cost is
    // 100. We think this happens because the back-links to parents are
    // not established. So, give the subset another chance to figure out
    // its cost.
    try {
      propagateCostImprovements(rel);
    } catch (CyclicMetadataException e) {
      // ignore
    }

    if (ruleDriver != null) {
      ruleDriver.onProduce(rel, subset);
    }

    return subset;
  }

  private RelSubset registerSubset(
      @Nullable RelSet set,
      RelSubset subset) {
    if ((set != subset.set)
        && (set != null)
        && (set.equivalentSet == null)) {
      LOGGER.trace("Register #{} {}, and merge sets", subset.getId(), subset);
      merge(set, subset.set);
    }
    return canonize(subset);
  }

  // implement RelOptPlanner
  @Override public void registerMetadataProviders(List<RelMetadataProvider> list) {
    list.add(0, new VolcanoRelMetadataProvider());
  }

  // implement RelOptPlanner
  @Override public long getRelMetadataTimestamp(RelNode rel) {
    RelSubset subset = getSubset(rel);
    if (subset == null) {
      return 0;
    } else {
      return subset.timestamp;
    }
  }

  /**
   * Normalizes references to subsets within the string representation of a
   * plan.
   *
   * <p>This is useful when writing tests: it helps to ensure that tests don't
   * break when an extra rule is introduced that generates a new subset and
   * causes subsequent subset numbers to be off by one.
   *
   * <p>For example,
   *
   * <blockquote>
   * FennelAggRel.FENNEL_EXEC(child=Subset#17.FENNEL_EXEC,groupCount=1,
   * EXPR$1=COUNT())<br>
   * &nbsp;&nbsp;FennelSortRel.FENNEL_EXEC(child=Subset#2.FENNEL_EXEC,
   * key=[0], discardDuplicates=false)<br>
   * &nbsp;&nbsp;&nbsp;&nbsp;FennelCalcRel.FENNEL_EXEC(
   * child=Subset#4.FENNEL_EXEC, expr#0..8={inputs}, expr#9=3456,
   * DEPTNO=$t7, $f0=$t9)<br>
   * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;MockTableImplRel.FENNEL_EXEC(
   * table=[CATALOG, SALES, EMP])</blockquote>
   *
   * <p>becomes
   *
   * <blockquote>
   * FennelAggRel.FENNEL_EXEC(child=Subset#{0}.FENNEL_EXEC, groupCount=1,
   * EXPR$1=COUNT())<br>
   * &nbsp;&nbsp;FennelSortRel.FENNEL_EXEC(child=Subset#{1}.FENNEL_EXEC,
   * key=[0], discardDuplicates=false)<br>
   * &nbsp;&nbsp;&nbsp;&nbsp;FennelCalcRel.FENNEL_EXEC(
   * child=Subset#{2}.FENNEL_EXEC,expr#0..8={inputs},expr#9=3456,DEPTNO=$t7,
   * $f0=$t9)<br>
   * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;MockTableImplRel.FENNEL_EXEC(
   * table=[CATALOG, SALES, EMP])</blockquote>
   *
   * <p>Returns null if and only if {@code plan} is null.
   *
   * @param plan Plan
   * @return Normalized plan
   */
  public static @PolyNull String normalizePlan(@PolyNull String plan) {
    if (plan == null) {
      return null;
    }
    final Pattern poundDigits = Pattern.compile("Subset#[0-9]+\\.");
    int i = 0;
    while (true) {
      final Matcher matcher = poundDigits.matcher(plan);
      if (!matcher.find()) {
        return plan;
      }
      final String token = matcher.group(); // e.g. "Subset#23."
      plan = plan.replace(token, "Subset#{" + i++ + "}.");
    }
  }

  /**
   * Sets whether this planner is locked. A locked planner does not accept
   * new rules. {@link #addRule(org.apache.calcite.plan.RelOptRule)} will do
   * nothing and return false.
   *
   * @param locked Whether planner is locked
   */
  public void setLocked(boolean locked) {
    this.locked = locked;
  }

  /**
   * Decide whether a rule is logical or not.
   * @param rel The specific rel node
   * @return True if the relnode is a logical node
   */
  @API(since = "1.24", status = API.Status.EXPERIMENTAL)
  public boolean isLogical(RelNode rel) {
    return !(rel instanceof PhysicalNode)
        && rel.getConvention() != rootConvention;
  }

  /**
   * Checks whether a rule match is a substitution rule match.
   *
   * @param match The rule match to check
   * @return True if the rule match is a substitution rule match
   */
  @API(since = "1.24", status = API.Status.EXPERIMENTAL)
  protected boolean isSubstituteRule(VolcanoRuleCall match) {
    return match.getRule() instanceof SubstitutionRule;
  }

  /**
   * Checks whether a rule match is a transformation rule match.
   *
   * @param match The rule match to check
   * @return True if the rule match is a transformation rule match
   */
  @API(since = "1.24", status = API.Status.EXPERIMENTAL)
  protected boolean isTransformationRule(VolcanoRuleCall match) {
    if (match.getRule() instanceof SubstitutionRule) {
      return true;
    }
    if (match.getRule() instanceof ConverterRule
        && match.getRule().getOutTrait() == rootConvention) {
      return false;
    }
    return match.getRule().getOperand().trait == Convention.NONE
        || match.getRule().getOperand().trait == null;
  }


  /**
   * Gets the lower bound cost of a relational operator.
   *
   * @param rel The rel node
   * @return The lower bound cost of the given rel. The value is ensured NOT NULL.
   */
  @API(since = "1.24", status = API.Status.EXPERIMENTAL)
  protected RelOptCost getLowerBound(RelNode rel) {
    RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RelOptCost lowerBound = mq.getLowerBoundCost(rel, this);
    if (lowerBound == null) {
      return zeroCost;
    }
    return lowerBound;
  }

  /**
   * Gets the upper bound of its inputs.
   * Allow users to overwrite this method as some implementations may have
   * different cost model on some RelNodes, like Spool.
   */
  @API(since = "1.24", status = API.Status.EXPERIMENTAL)
  protected RelOptCost upperBoundForInputs(
      RelNode mExpr, RelOptCost upperBound) {
    if (!upperBound.isInfinite()) {
      RelOptCost rootCost = mExpr.getCluster()
          .getMetadataQuery().getNonCumulativeCost(mExpr);
      if (rootCost != null && !rootCost.isInfinite()) {
        return upperBound.minus(rootCost);
      }
    }
    return upperBound;
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * A rule call which defers its actions. Whereas {@link RelOptRuleCall}
   * invokes the rule when it finds a match, a <code>DeferringRuleCall</code>
   * creates a {@link VolcanoRuleMatch} which can be invoked later.
   */
  private static class DeferringRuleCall extends VolcanoRuleCall {
    DeferringRuleCall(
        VolcanoPlanner planner,
        RelOptRuleOperand operand) {
      super(planner, operand);
    }

    /**
     * Rather than invoking the rule (as the base method does), creates a
     * {@link VolcanoRuleMatch} which can be invoked later.
     */
    @Override protected void onMatch() {
      final VolcanoRuleMatch match =
          new VolcanoRuleMatch(
              volcanoPlanner,
              getOperand0(),
              rels,
              nodeInputs);
      volcanoPlanner.ruleDriver.getRuleQueue().addMatch(match);
    }
  }

  /**
   * Where a RelNode came from.
   */
  abstract static class Provenance {
    public static final Provenance EMPTY = new UnknownProvenance();
  }

  /**
   * We do not know where this RelNode came from. Probably created by hand,
   * or by sql-to-rel converter.
   */
  private static class UnknownProvenance extends Provenance {
  }

  /**
   * A RelNode that came directly from another RelNode via a copy.
   */
  static class DirectProvenance extends Provenance {
    final RelNode source;

    DirectProvenance(RelNode source) {
      this.source = source;
    }
  }

  /**
   * A RelNode that came via the firing of a rule.
   */
  static class RuleProvenance extends Provenance {
    final RelOptRule rule;
    final ImmutableList<RelNode> rels;
    final int callId;

    RuleProvenance(RelOptRule rule, ImmutableList<RelNode> rels, int callId) {
      this.rule = rule;
      this.rels = rels;
      this.callId = callId;
    }
  }
}
