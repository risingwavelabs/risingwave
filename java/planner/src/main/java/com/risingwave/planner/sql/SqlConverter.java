package com.risingwave.planner.sql;

import static java.util.Objects.requireNonNull;

import com.risingwave.common.datatype.RisingWaveTypeFactory;
import com.risingwave.planner.cost.RisingWaveCostFactory;
import com.risingwave.planner.planner.PlannerContext;
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

public class SqlConverter {
  private final SqlValidator validator;
  private final SqlToRelConverter sqlToRelConverter;
  private final PlannerContext context;

  private SqlConverter(
      SqlValidator validator, SqlToRelConverter sqlToRelConverter, PlannerContext context) {
    this.validator = requireNonNull(validator, "validator can't be null!");
    this.sqlToRelConverter =
        requireNonNull(sqlToRelConverter, "sql to rel converter can't be null!");
    this.context = requireNonNull(context, "context can't be null!");
  }

  public RelRoot toRel(SqlNode ast) {
    SqlNode validatedSqlNode = validator.validate(ast);
    return sqlToRelConverter.convertQuery(validatedSqlNode, false, true);
  }

  public static Builder builder(PlannerContext context, SchemaPlus rootSchema) {
    return new Builder(context, rootSchema);
  }

  public static class Builder {
    private final PlannerContext context;
    private final SchemaPlus rootSchema;

    private final RisingWaveCostFactory costFactory = new RisingWaveCostFactory();
    private final RisingWaveTypeFactory typeFactory = new RisingWaveTypeFactory();

    private List<String> defaultSchema = Collections.emptyList();
    private SqlToRelConverter.Config config = SqlToRelConverter.config();
    private VolcanoPlanner planner = null;
    private RelOptCluster cluster = null;

    private Builder(PlannerContext context, SchemaPlus rootSchema) {
      this.context = context;
      this.rootSchema = rootSchema;
    }

    public Builder withDefaultSchema(List<String> newDefaultSchema) {
      defaultSchema = requireNonNull(newDefaultSchema, "Default schema can't be null!");
      return this;
    }

    public Builder withSql2RelConverterConfig(SqlToRelConverter.Config config) {
      this.config = requireNonNull(config, "config can't be null!");
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
      SqlToRelConverter sql2RelConverter =
          new SqlToRelConverter(
              catalogReader, validator, catalogReader, cluster, sqlRexConvertletTable, config);
      return new SqlConverter(validator, sql2RelConverter, context);
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
      }
    }

    private void initCluster() {
      if (cluster == null) {
        cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        //        JaninoRelMetadataProvider relMetadataProvider =
        // Utilities.registerJaninoRelMetadataProvider();
        //        cluster.setMetadataProvider(relMetadataProvider);
      }
    }
  }
}
