package com.risingwave.planner.rules.physical;

import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;
import static com.risingwave.planner.rel.physical.RisingWaveBatchPhyRel.BATCH_PHYSICAL;

import com.risingwave.catalog.MaterializedViewCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.planner.rel.logical.RwLogicalScan;
import com.risingwave.planner.rel.physical.RwBatchScan;
import com.risingwave.planner.rel.physical.RwBatchSourceScan;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Rule to convert Scan operator to different kinds of physical scan */
public class BatchScanConverterRule extends ConverterRule {

  public static final BatchScanConverterRule INSTANCE =
      Config.INSTANCE
          .withInTrait(LOGICAL)
          .withOutTrait(BATCH_PHYSICAL)
          .withRuleFactory(BatchScanConverterRule::new)
          .withOperandSupplier(t -> t.operand(RwLogicalScan.class).anyInputs())
          .withDescription("Converting logical filter scan to batch scan.")
          .as(Config.class)
          .toRule(BatchScanConverterRule.class);

  public BatchScanConverterRule(Config config) {
    super(config);
  }

  @Override
  public @Nullable RelNode convert(RelNode rel) {
    RwLogicalScan source = (RwLogicalScan) rel;
    TableCatalog table = source.getTable().unwrapOrThrow(TableCatalog.class);
    if (table.isSource()) {
      return RwBatchSourceScan.create(
          source.getCluster(), source.getTraitSet(), source.getTable(), source.getColumnIds());
    } else {
      RelTraitSet scanTraitSet = source.getTraitSet();

      if (table.isMaterializedView()) {
        var view = (MaterializedViewCatalog) table;
        if (view.getCollation() != null) {
          scanTraitSet = scanTraitSet.plus(view.getCollation());
        }
      }

      return RwBatchScan.create(
          source.getCluster(), scanTraitSet, source.getTable(), source.getColumnIds());
    }
  }
}
