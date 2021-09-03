package com.risingwave.planner;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.risingwave.catalog.TableCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class CreateTableTest extends SqlTestBase {

  @BeforeEach
  public void init() {
    super.initEnv();
  }

  @Test
  @DisplayName("Create table column without not null should be null")
  public void testDefaultNullability() {
    String sql = "create table t1 (v1 int)";
    executeSql(sql);

    assertTrue(
        executionContext
            .getCatalogService()
            .getTableChecked(TableCatalog.TableName.of(TEST_DB_NAME, TEST_SCHEMA_NAME, "t1"))
            .getColumnChecked("v1")
            .getDesc()
            .getDataType()
            .isNullable());
  }

  @Test
  @DisplayName("Create table column with not null should be not null")
  public void testCreateColumnNotNull() {
    String sql = "create table t1 (v1 int not null)";
    executeSql(sql);

    assertFalse(
        executionContext
            .getCatalogService()
            .getTableChecked(TableCatalog.TableName.of(TEST_DB_NAME, TEST_SCHEMA_NAME, "t1"))
            .getColumnChecked("v1")
            .getDesc()
            .getDataType()
            .isNullable());
  }
}
