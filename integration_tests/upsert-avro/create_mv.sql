CREATE MATERIALIZED VIEW items_view AS
SELECT
    id,
    op_type,
    "ID",
    "CLASS_ID",
    "ITEM_ID",
    "ATTR_ID"
FROM
    items;