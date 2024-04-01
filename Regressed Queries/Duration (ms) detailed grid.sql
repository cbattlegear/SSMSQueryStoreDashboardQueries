(@results_row_count int,@recent_start_time datetimeoffset(7),@recent_end_time datetimeoffset(7),@history_start_time datetimeoffset(7),@history_end_time datetimeoffset(7),@min_exec_count bigint)WITH
wait_stats AS
(
SELECT
    ws.plan_id plan_id,
    ws.wait_category,
    ROUND(CONVERT(float, SUM(ws.total_query_wait_time_ms)/SUM(ws.total_query_wait_time_ms/ws.avg_query_wait_time_ms))*1,2) avg_query_wait_time,
    ROUND(CONVERT(float, MIN(ws.min_query_wait_time_ms))*1,2) min_query_wait_time,
    ROUND(CONVERT(float, MAX(ws.max_query_wait_time_ms))*1,2) max_query_wait_time,
    ROUND(CONVERT(float, SQRT( SUM(ws.stdev_query_wait_time_ms*ws.stdev_query_wait_time_ms*(ws.total_query_wait_time_ms/ws.avg_query_wait_time_ms))/SUM(ws.total_query_wait_time_ms/ws.avg_query_wait_time_ms)))*1,2) stdev_query_wait_time,
    ROUND(CONVERT(float, SUM(ws.total_query_wait_time_ms))*1,2) total_query_wait_time,
    CAST(ROUND(SUM(ws.total_query_wait_time_ms/ws.avg_query_wait_time_ms),0) AS BIGINT) count_executions,
    MAX(itvl.end_time) last_execution_time,
    MIN(itvl.start_time) first_execution_time
FROM sys.query_store_wait_stats ws
    JOIN sys.query_store_runtime_stats_interval itvl ON itvl.runtime_stats_interval_id = ws.runtime_stats_interval_id
WHERE NOT (itvl.start_time > @history_end_time OR itvl.end_time < @history_start_time)
GROUP BY ws.plan_id, ws.runtime_stats_interval_id, ws.wait_category
),
wait_stats_hist AS
(
SELECT
    p.query_id query_id,
    ROUND(CONVERT(float, SUM(ws.avg_query_wait_time*ws.count_executions))*1,2) total_query_wait_time,
    MAX(ws.count_executions) count_executions,
    COUNT(distinct p.plan_id) num_plans
FROM wait_stats ws
    JOIN sys.query_store_plan p ON p.plan_id = ws.plan_id
WHERE NOT (ws.first_execution_time > @history_end_time OR ws.last_execution_time < @history_start_time)
GROUP BY p.query_id
),
other_hist AS
(
SELECT
    p.query_id query_id,
    ROUND(CONVERT(float, SUM(rs.avg_duration*rs.count_executions))*0.001,2) total_duration,
    ROUND(CONVERT(float, SUM(rs.avg_cpu_time*rs.count_executions))*0.001,2) total_cpu_time,
    ROUND(CONVERT(float, SUM(rs.avg_logical_io_reads*rs.count_executions))*8,2) total_logical_io_reads,
    ROUND(CONVERT(float, SUM(rs.avg_logical_io_writes*rs.count_executions))*8,2) total_logical_io_writes,
    ROUND(CONVERT(float, SUM(rs.avg_physical_io_reads*rs.count_executions))*8,2) total_physical_io_reads,
    ROUND(CONVERT(float, SUM(rs.avg_clr_time*rs.count_executions))*0.001,2) total_clr_time,
    ROUND(CONVERT(float, SUM(rs.avg_dop*rs.count_executions))*1,0) total_dop,
    ROUND(CONVERT(float, SUM(rs.avg_query_max_used_memory*rs.count_executions))*8,2) total_query_max_used_memory,
    ROUND(CONVERT(float, SUM(rs.avg_rowcount*rs.count_executions))*1,0) total_rowcount,
    ROUND(CONVERT(float, SUM(rs.avg_log_bytes_used*rs.count_executions))*0.0009765625,2) total_log_bytes_used,
    ROUND(CONVERT(float, SUM(rs.avg_tempdb_space_used*rs.count_executions))*8,2) total_tempdb_space_used,
    SUM(rs.count_executions) count_executions,
    COUNT(distinct p.plan_id) num_plans
FROM sys.query_store_runtime_stats rs
    JOIN sys.query_store_plan p ON p.plan_id = rs.plan_id
WHERE NOT (rs.first_execution_time > @history_end_time OR rs.last_execution_time < @history_start_time)
GROUP BY p.query_id
),
hist AS
(
SELECT
    other_hist.query_id,
    other_hist.total_duration total_duration,
    other_hist.total_cpu_time total_cpu_time,
    other_hist.total_logical_io_reads total_logical_io_reads,
    other_hist.total_logical_io_writes total_logical_io_writes,
    other_hist.total_physical_io_reads total_physical_io_reads,
    other_hist.total_clr_time total_clr_time,
    other_hist.total_dop total_dop,
    other_hist.total_query_max_used_memory total_query_max_used_memory,
    other_hist.total_rowcount total_rowcount,
    other_hist.total_log_bytes_used total_log_bytes_used,
    other_hist.total_tempdb_space_used total_tempdb_space_used,
    ISNULL(wait_stats_hist.total_query_wait_time, 0) total_query_wait_time,
    other_hist.count_executions,
    wait_stats_hist.count_executions wait_stats_count_executions,
    other_hist.num_plans
FROM other_hist
    LEFT JOIN wait_stats_hist ON wait_stats_hist.query_id = other_hist.query_id
),
wait_stats_recent AS
(
SELECT
    p.query_id query_id,
    ROUND(CONVERT(float, SUM(ws.avg_query_wait_time*ws.count_executions))*1,2) total_query_wait_time,
    MAX(ws.count_executions) count_executions,
    COUNT(distinct p.plan_id) num_plans
FROM wait_stats ws
    JOIN sys.query_store_plan p ON p.plan_id = ws.plan_id
WHERE NOT (ws.first_execution_time > @recent_end_time OR ws.last_execution_time < @recent_start_time)
GROUP BY p.query_id
),
other_recent AS
(
SELECT
    p.query_id query_id,
    ROUND(CONVERT(float, SUM(rs.avg_duration*rs.count_executions))*0.001,2) total_duration,
    ROUND(CONVERT(float, SUM(rs.avg_cpu_time*rs.count_executions))*0.001,2) total_cpu_time,
    ROUND(CONVERT(float, SUM(rs.avg_logical_io_reads*rs.count_executions))*8,2) total_logical_io_reads,
    ROUND(CONVERT(float, SUM(rs.avg_logical_io_writes*rs.count_executions))*8,2) total_logical_io_writes,
    ROUND(CONVERT(float, SUM(rs.avg_physical_io_reads*rs.count_executions))*8,2) total_physical_io_reads,
    ROUND(CONVERT(float, SUM(rs.avg_clr_time*rs.count_executions))*0.001,2) total_clr_time,
    ROUND(CONVERT(float, SUM(rs.avg_dop*rs.count_executions))*1,0) total_dop,
    ROUND(CONVERT(float, SUM(rs.avg_query_max_used_memory*rs.count_executions))*8,2) total_query_max_used_memory,
    ROUND(CONVERT(float, SUM(rs.avg_rowcount*rs.count_executions))*1,0) total_rowcount,
    ROUND(CONVERT(float, SUM(rs.avg_log_bytes_used*rs.count_executions))*0.0009765625,2) total_log_bytes_used,
    ROUND(CONVERT(float, SUM(rs.avg_tempdb_space_used*rs.count_executions))*8,2) total_tempdb_space_used,
    SUM(rs.count_executions) count_executions,
    COUNT(distinct p.plan_id) num_plans
FROM sys.query_store_runtime_stats rs
    JOIN sys.query_store_plan p ON p.plan_id = rs.plan_id
WHERE NOT (rs.first_execution_time > @recent_end_time OR rs.last_execution_time < @recent_start_time)
GROUP BY p.query_id
),
recent AS
(
SELECT
    other_recent.query_id,
    other_recent.total_duration total_duration,
    other_recent.total_cpu_time total_cpu_time,
    other_recent.total_logical_io_reads total_logical_io_reads,
    other_recent.total_logical_io_writes total_logical_io_writes,
    other_recent.total_physical_io_reads total_physical_io_reads,
    other_recent.total_clr_time total_clr_time,
    other_recent.total_dop total_dop,
    other_recent.total_query_max_used_memory total_query_max_used_memory,
    other_recent.total_rowcount total_rowcount,
    other_recent.total_log_bytes_used total_log_bytes_used,
    other_recent.total_tempdb_space_used total_tempdb_space_used,
    ISNULL(wait_stats_recent.total_query_wait_time, 0) total_query_wait_time,
    other_recent.count_executions,
    wait_stats_recent.count_executions wait_stats_count_executions,
    other_recent.num_plans
FROM other_recent
    LEFT JOIN wait_stats_recent ON wait_stats_recent.query_id = other_recent.query_id
)
SELECT TOP (@results_row_count)
    results.query_id query_id,
    results.object_id object_id,
    ISNULL(OBJECT_NAME(results.object_id),'') object_name,
    results.query_sql_text query_sql_text,
    results.additional_duration_workload additional_duration_workload,
    results.total_duration_recent total_duration_recent,
    results.total_duration_hist total_duration_hist,
    results.additional_cpu_time_workload additional_cpu_time_workload,
    results.total_cpu_time_recent total_cpu_time_recent,
    results.total_cpu_time_hist total_cpu_time_hist,
    results.additional_logical_io_reads_workload additional_logical_io_reads_workload,
    results.total_logical_io_reads_recent total_logical_io_reads_recent,
    results.total_logical_io_reads_hist total_logical_io_reads_hist,
    results.additional_logical_io_writes_workload additional_logical_io_writes_workload,
    results.total_logical_io_writes_recent total_logical_io_writes_recent,
    results.total_logical_io_writes_hist total_logical_io_writes_hist,
    results.additional_physical_io_reads_workload additional_physical_io_reads_workload,
    results.total_physical_io_reads_recent total_physical_io_reads_recent,
    results.total_physical_io_reads_hist total_physical_io_reads_hist,
    results.additional_clr_time_workload additional_clr_time_workload,
    results.total_clr_time_recent total_clr_time_recent,
    results.total_clr_time_hist total_clr_time_hist,
    results.additional_dop_workload additional_dop_workload,
    results.total_dop_recent total_dop_recent,
    results.total_dop_hist total_dop_hist,
    results.additional_query_max_used_memory_workload additional_query_max_used_memory_workload,
    results.total_query_max_used_memory_recent total_query_max_used_memory_recent,
    results.total_query_max_used_memory_hist total_query_max_used_memory_hist,
    results.additional_rowcount_workload additional_rowcount_workload,
    results.total_rowcount_recent total_rowcount_recent,
    results.total_rowcount_hist total_rowcount_hist,
    results.additional_log_bytes_used_workload additional_log_bytes_used_workload,
    results.total_log_bytes_used_recent total_log_bytes_used_recent,
    results.total_log_bytes_used_hist total_log_bytes_used_hist,
    results.additional_tempdb_space_used_workload additional_tempdb_space_used_workload,
    results.total_tempdb_space_used_recent total_tempdb_space_used_recent,
    results.total_tempdb_space_used_hist total_tempdb_space_used_hist,
    results.additional_query_wait_time_workload additional_query_wait_time_workload,
    results.total_query_wait_time_recent total_query_wait_time_recent,
    results.total_query_wait_time_hist total_query_wait_time_hist,
    ISNULL(results.count_executions_recent, 0) count_executions_recent,
    ISNULL(results.count_executions_hist, 0) count_executions_hist,
    queries.num_plans num_plans
FROM
(
SELECT
    hist.query_id query_id,
    q.object_id object_id,
    qt.query_sql_text query_sql_text,
    ROUND(CONVERT(float, recent.total_duration/recent.wait_stats_count_executions-hist.total_duration/hist.wait_stats_count_executions)*(recent.wait_stats_count_executions), 2) additional_duration_workload,
    ROUND(recent.total_duration, 2) total_duration_recent,
    ROUND(hist.total_duration, 2) total_duration_hist,
    ROUND(CONVERT(float, recent.total_cpu_time/recent.wait_stats_count_executions-hist.total_cpu_time/hist.wait_stats_count_executions)*(recent.wait_stats_count_executions), 2) additional_cpu_time_workload,
    ROUND(recent.total_cpu_time, 2) total_cpu_time_recent,
    ROUND(hist.total_cpu_time, 2) total_cpu_time_hist,
    ROUND(CONVERT(float, recent.total_logical_io_reads/recent.wait_stats_count_executions-hist.total_logical_io_reads/hist.wait_stats_count_executions)*(recent.wait_stats_count_executions), 2) additional_logical_io_reads_workload,
    ROUND(recent.total_logical_io_reads, 2) total_logical_io_reads_recent,
    ROUND(hist.total_logical_io_reads, 2) total_logical_io_reads_hist,
    ROUND(CONVERT(float, recent.total_logical_io_writes/recent.wait_stats_count_executions-hist.total_logical_io_writes/hist.wait_stats_count_executions)*(recent.wait_stats_count_executions), 2) additional_logical_io_writes_workload,
    ROUND(recent.total_logical_io_writes, 2) total_logical_io_writes_recent,
    ROUND(hist.total_logical_io_writes, 2) total_logical_io_writes_hist,
    ROUND(CONVERT(float, recent.total_physical_io_reads/recent.wait_stats_count_executions-hist.total_physical_io_reads/hist.wait_stats_count_executions)*(recent.wait_stats_count_executions), 2) additional_physical_io_reads_workload,
    ROUND(recent.total_physical_io_reads, 2) total_physical_io_reads_recent,
    ROUND(hist.total_physical_io_reads, 2) total_physical_io_reads_hist,
    ROUND(CONVERT(float, recent.total_clr_time/recent.wait_stats_count_executions-hist.total_clr_time/hist.wait_stats_count_executions)*(recent.wait_stats_count_executions), 2) additional_clr_time_workload,
    ROUND(recent.total_clr_time, 2) total_clr_time_recent,
    ROUND(hist.total_clr_time, 2) total_clr_time_hist,
    ROUND(CONVERT(float, recent.total_dop/recent.wait_stats_count_executions-hist.total_dop/hist.wait_stats_count_executions)*(recent.wait_stats_count_executions), 2) additional_dop_workload,
    ROUND(recent.total_dop, 2) total_dop_recent,
    ROUND(hist.total_dop, 2) total_dop_hist,
    ROUND(CONVERT(float, recent.total_query_max_used_memory/recent.wait_stats_count_executions-hist.total_query_max_used_memory/hist.wait_stats_count_executions)*(recent.wait_stats_count_executions), 2) additional_query_max_used_memory_workload,
    ROUND(recent.total_query_max_used_memory, 2) total_query_max_used_memory_recent,
    ROUND(hist.total_query_max_used_memory, 2) total_query_max_used_memory_hist,
    ROUND(CONVERT(float, recent.total_rowcount/recent.wait_stats_count_executions-hist.total_rowcount/hist.wait_stats_count_executions)*(recent.wait_stats_count_executions), 2) additional_rowcount_workload,
    ROUND(recent.total_rowcount, 2) total_rowcount_recent,
    ROUND(hist.total_rowcount, 2) total_rowcount_hist,
    ROUND(CONVERT(float, recent.total_log_bytes_used/recent.wait_stats_count_executions-hist.total_log_bytes_used/hist.wait_stats_count_executions)*(recent.wait_stats_count_executions), 2) additional_log_bytes_used_workload,
    ROUND(recent.total_log_bytes_used, 2) total_log_bytes_used_recent,
    ROUND(hist.total_log_bytes_used, 2) total_log_bytes_used_hist,
    ROUND(CONVERT(float, recent.total_tempdb_space_used/recent.wait_stats_count_executions-hist.total_tempdb_space_used/hist.wait_stats_count_executions)*(recent.wait_stats_count_executions), 2) additional_tempdb_space_used_workload,
    ROUND(recent.total_tempdb_space_used, 2) total_tempdb_space_used_recent,
    ROUND(hist.total_tempdb_space_used, 2) total_tempdb_space_used_hist,
    ROUND(CONVERT(float, recent.total_query_wait_time/recent.wait_stats_count_executions-hist.total_query_wait_time/hist.wait_stats_count_executions)*(recent.wait_stats_count_executions), 2) additional_query_wait_time_workload,
    ROUND(recent.total_query_wait_time, 2) total_query_wait_time_recent,
    ROUND(hist.total_query_wait_time, 2) total_query_wait_time_hist,
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
ORDER BY additional_duration_workload DESC
OPTION (MERGE JOIN) 