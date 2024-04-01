(@results_row_count int,@recent_start_time datetimeoffset(7),@recent_end_time datetimeoffset(7),@history_start_time datetimeoffset(7),@history_end_time datetimeoffset(7),@min_exec_count bigint)WITH 
hist AS
(
SELECT
    p.query_id query_id,
    ROUND(CONVERT(float, SUM(rs.avg_duration*rs.count_executions))*0.001,2) total_duration,
    SUM(rs.count_executions) count_executions,
    COUNT(distinct p.plan_id) num_plans
FROM sys.query_store_runtime_stats rs
    JOIN sys.query_store_plan p ON p.plan_id = rs.plan_id
WHERE NOT (rs.first_execution_time > @history_end_time OR rs.last_execution_time < @history_start_time)
GROUP BY p.query_id
),
recent AS
(
SELECT
    p.query_id query_id,
    ROUND(CONVERT(float, SUM(rs.avg_duration*rs.count_executions))*0.001,2) total_duration,
    SUM(rs.count_executions) count_executions,
    COUNT(distinct p.plan_id) num_plans
FROM sys.query_store_runtime_stats rs
    JOIN sys.query_store_plan p ON p.plan_id = rs.plan_id
WHERE NOT (rs.first_execution_time > @recent_end_time OR rs.last_execution_time < @recent_start_time)
GROUP BY p.query_id
)
SELECT TOP (@results_row_count)
    results.query_id query_id,
    results.object_id object_id,
    ISNULL(OBJECT_NAME(results.object_id),'') object_name,
    results.query_sql_text query_sql_text,
    results.additional_duration_workload additional_duration_workload,
    results.total_duration_recent total_duration_recent,
    results.total_duration_hist total_duration_hist,
    ISNULL(results.count_executions_recent, 0) count_executions_recent,
    ISNULL(results.count_executions_hist, 0) count_executions_hist,
    queries.num_plans num_plans
FROM
(
SELECT
    hist.query_id query_id,
    q.object_id object_id,
    qt.query_sql_text query_sql_text,
    ROUND(CONVERT(float, recent.total_duration/recent.count_executions-hist.total_duration/hist.count_executions)*(recent.count_executions), 2) additional_duration_workload,
    ROUND(recent.total_duration, 2) total_duration_recent,
    ROUND(hist.total_duration, 2) total_duration_hist,
    recent.count_executions count_executions_recent,
    hist.count_executions count_executions_hist
FROM hist
    JOIN recent ON hist.query_id = recent.query_id
    JOIN sys.query_store_query q ON q.query_id = hist.query_id
    JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
WHERE
    recent.count_executions >= @min_exec_count
) AS results
JOIN
(
SELECT
    p.query_id query_id,
    COUNT(distinct p.plan_id) num_plans
FROM sys.query_store_plan p
GROUP BY p.query_id
HAVING COUNT(distinct p.plan_id) >= 1
) AS queries ON queries.query_id = results.query_id
WHERE additional_duration_workload > 0
ORDER BY additional_duration_workload DESC
OPTION (MERGE JOIN) 