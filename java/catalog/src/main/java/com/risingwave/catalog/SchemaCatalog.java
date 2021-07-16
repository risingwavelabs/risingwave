package com.risingwave.catalog;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SchemaCatalog extends AbstractNonLeafEntity<TableCatalog> implements Schema {
  public SchemaCatalog(int id, String name, Collection<TableCatalog> tables) {
    super(id, name, tables);
  }

  @Override
  @Nullable
  public Table getTable(String name) {
    return getChildByName(name);
  }

  @Override
  public Set<String> getTableNames() {
    return getChildrenNames();
  }

  @Override
  @Nullable
  public RelProtoDataType getType(String name) {
    return null;
  }

  @Override
  public Set<String> getTypeNames() {
    return Collections.emptySet();
  }

  @Override
  public Collection<Function> getFunctions(String name) {
    return Collections.emptyList();
  }

  @Override
  public Set<String> getFunctionNames() {
    return Collections.emptySet();
  }

  @Override
  @Nullable
  public Schema getSubSchema(String name) {
    return null;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return Collections.emptySet();
  }

  @Override
  public Expression getExpression(@Nullable SchemaPlus parentSchema, String name) {
    return null;
  }

  @Override
  public boolean isMutable() {
    return false;
  }

  @Override
  public Schema snapshot(SchemaVersion version) {
    return this;
  }
}
