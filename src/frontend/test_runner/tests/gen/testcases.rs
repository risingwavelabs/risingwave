
#[tokio::test]
async fn test_basic_query() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("basic_query.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("basic_query", &file_content)
        .await;
}
                        

#[tokio::test]
async fn test_common_table_expressions() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("common_table_expressions.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("common_table_expressions", &file_content)
        .await;
}
                        

#[tokio::test]
async fn test_distribution_derive() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("distribution_derive.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("distribution_derive", &file_content)
        .await;
}
                        

#[tokio::test]
async fn test_predicate_pushdown() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("predicate_pushdown.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("predicate_pushdown", &file_content)
        .await;
}
                        

#[tokio::test]
async fn test_mv_on_mv() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("mv_on_mv.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("mv_on_mv", &file_content)
        .await;
}
                        

#[tokio::test]
async fn test_stream_proto() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("stream_proto.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("stream_proto", &file_content)
        .await;
}
                        

#[tokio::test]
async fn test_subquery_expr_correlated() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("subquery_expr_correlated.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("subquery_expr_correlated", &file_content)
        .await;
}
                        

#[tokio::test]
async fn test_array() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("array.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("array", &file_content)
        .await;
}
                        

#[tokio::test]
async fn test_types() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("types.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("types", &file_content)
        .await;
}
                        

#[tokio::test]
async fn test_column_pruning() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("column_pruning.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("column_pruning", &file_content)
        .await;
}
                        

#[tokio::test]
async fn test_order_by() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("order_by.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("order_by", &file_content)
        .await;
}
                        

#[tokio::test]
async fn test_subquery_expr() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("subquery_expr.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("subquery_expr", &file_content)
        .await;
}
                        

#[tokio::test]
async fn test_subquery() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("subquery.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("subquery", &file_content)
        .await;
}
                        

#[tokio::test]
async fn test_pk_derive() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("pk_derive.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("pk_derive", &file_content)
        .await;
}
                        

#[tokio::test]
async fn test_agg() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("agg.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("agg", &file_content)
        .await;
}
                        

#[tokio::test]
async fn test_nexmark() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("nexmark.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("nexmark", &file_content)
        .await;
}
                        

#[tokio::test]
async fn test_index() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("index.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("index", &file_content)
        .await;
}
                        

#[tokio::test]
async fn test_time_window() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("time_window.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("time_window", &file_content)
        .await;
}
                        

#[tokio::test]
async fn test_struct_query() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("struct_query.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("struct_query", &file_content)
        .await;
}
                        

#[tokio::test]
async fn test_join() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("join.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("join", &file_content)
        .await;
}
                        

#[tokio::test]
async fn test_limit() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("limit.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("limit", &file_content)
        .await;
}
                        

#[tokio::test]
async fn test_insert() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("insert.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("insert", &file_content)
        .await;
}
                        

#[tokio::test]
async fn test_tpch() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("tpch.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("tpch", &file_content)
        .await;
}
                        

#[tokio::test]
async fn test_expr() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("expr.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("expr", &file_content)
        .await;
}
                        

#[tokio::test]
async fn test_update() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join("update.yaml");
    let file_content = tokio::fs::read_to_string(path).await.unwrap();
    risingwave_frontend_test_runner::run_test_file("update", &file_content)
        .await;
}
                        
