package com.risingwave.planner.sql;

import static java.util.Objects.requireNonNull;

import com.risingwave.common.datatype.RisingWaveTypeFactory;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.planner.cost.RisingWaveCostFactory;
import com.risingwave.planner.metadata.RisingWaveRelMetadataProvider;
import com.risingwave.planner.metadata.RisingWaveRelMetadataQuery;
import com.risingwave.planner.rel.common.dist.RwDistributionTraitDef;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;

/** SqlConverter */
public class SqlConverter {
  private final SqlValidator validator;
  private final SqlToRelConverter sqlToRelConverter;

  private SqlConverter(SqlValidator validator, SqlToRelConverter sqlToRelConverter) {
    this.validator = requireNonNull(validator, "validator can't be null!");
    this.sqlToRelConverter =
        requireNonNull(sqlToRelConverter, "sql to rel converter can't be null!");
  }

  public RelRoot toRel(SqlNode ast) {
    SqlNode validatedSqlNode = validator.validate(ast);
    return sqlToRelConverter.convertQuery(validatedSqlNode, false, true);
  }

  public SqlValidator getValidator() {
    return validator;
  }

  public static Builder builder(ExecutionContext context) {
    return new Builder(context);
  }

  /** SqlConverter Builder */
  public static class Builder {
    private final ExecutionContext context;
    private final SchemaPlus rootSchema;

    private final RisingWaveCostFactory costFactory = new RisingWaveCostFactory();
    private final RisingWaveTypeFactory typeFactory = RisingWaveTypeFactory.INSTANCE;

    private List<String> defaultSchema = Collections.emptyList();
    private SqlToRelConverter.Config config = SqlToRelConverter.config();
    private VolcanoPlanner planner = null;
    private RelOptCluster cluster = null;

    private boolean withExpand = true;

    private Builder(ExecutionContext context) {
      this.context = context;
      this.rootSchema = context.getCalciteRootSchema();
    }

    public Builder withDefaultSchema(List<String> newDefaultSchema) {
      defaultSchema = requireNonNull(newDefaultSchema, "Default schema can't be null!");
      return this;
    }

    public Builder withExpand(boolean doExpand) {
      withExpand = doExpand;
      return this;
    }

    public SqlConverter build() {

      RisingWaveCalciteCatalogReader catalogReader =
          new RisingWaveCalciteCatalogReader(rootSchema, defaultSchema, typeFactory);

      RisingWaveOperatorTable operatorTable = new RisingWaveOperatorTable();

      RisingWaveSqlValidator validator =
          new RisingWaveSqlValidator(operatorTable, catalogReader, typeFactory);

      RisingWaveConvertletTable sqlRexConvertletTable = new RisingWaveConvertletTable();

      initAll();
      this.config =
          this.config
              .addRelBuilderConfigTransform(c -> c.withSimplify(false))
              .withExpand(this.withExpand);

      SqlToRelConverter sql2RelConverter =
          new RisingWaveSqlToRelConverter(
              catalogReader, validator, catalogReader, cluster, sqlRexConvertletTable, config);
      return new SqlConverter(validator, sql2RelConverter);
    }

    private void initAll() {
      initPlanner();
      initCluster();
    }

    private void initPlanner() {
      if (planner == null) {
        planner = new VolcanoPlanner(costFactory, context);
        planner.clearRelTraitDefs();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        planner.addRelTraitDef(RwDistributionTraitDef.getInstance());
        // Use a no-op executor to disable frontend expression evaluation.
        planner.setExecutor(
            (rexBuilder, constExps, reducedValues) -> reducedValues.addAll(constExps));
      }
    }

    private void initCluster() {
      if (cluster == null) {
        cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        cluster.setMetadataProvider(RisingWaveRelMetadataProvider.getMetadataProvider());
        cluster.setMetadataQuerySupplier(RisingWaveRelMetadataQuery::instance);
      }
    }
  }
}
