CREATE MATERIALIZED VIEW student_view AS
SELECT
    id,
    name,
    avg_score,
    age,
    schema_version
FROM
    student
WHERE age > 10;