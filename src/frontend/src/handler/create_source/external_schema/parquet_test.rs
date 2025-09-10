// Test to validate that the parquet schema validation works

use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId};
use risingwave_common::types::DataType;

use super::parquet::check_parquet_type_compatibility;
use super::*;

#[tokio::test]
async fn test_parquet_type_compatibility() {
    // Test compatible types
    assert!(check_parquet_type_compatibility(&DataType::Int32, &DataType::Int32).unwrap());
    assert!(check_parquet_type_compatibility(&DataType::Varchar, &DataType::Varchar).unwrap());
    assert!(check_parquet_type_compatibility(&DataType::Float64, &DataType::Float64).unwrap());
    
    // Test incompatible types
    assert!(!check_parquet_type_compatibility(&DataType::Int32, &DataType::Varchar).unwrap());
    assert!(!check_parquet_type_compatibility(&DataType::Float64, &DataType::Int32).unwrap());
}

#[test]
fn test_parquet_column_catalog_creation() {
    let column_desc = ColumnDesc::named(
        "test_column".to_string(),
        ColumnId::new(1),
        DataType::Int32,
    );
    let column_catalog = ColumnCatalog {
        column_desc,
        is_hidden: false,
    };
    
    assert_eq!(column_catalog.name(), "test_column");
    assert_eq!(column_catalog.data_type(), &DataType::Int32);
    assert!(!column_catalog.is_hidden);
}