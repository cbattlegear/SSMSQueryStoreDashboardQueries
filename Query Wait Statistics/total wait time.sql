(@interval_start_time datetimeoffset(7),@interval_end_time datetimeoffset(7),@results_row_count int)SELECT TOP (@results_row_count)
    ws.wait_category wait_category,
    ws.wait_category_desc wait_category_desc,
    ROUND(CONVERT(float, SUM(ws.total_query_wait_time_ms)/SUM(ws.total_query_wait_time_ms/ws.avg_query_wait_time_ms))*1,2) avg_query_wait_time,
    ROUND(CONVERT(float, MIN(ws.min_query_wait_time_ms))*1,2) min_query_wait_time,
    ROUND(CONVERT(float, MAX(ws.max_query_wait_time_ms))*1,2) max_query_wait_time,
    ROUND(CONVERT(float, SQRT( SUM(ws.stdev_query_wait_time_ms*ws.stdev_query_wait_time_ms*(ws.total_query_wait_time_ms/ws.avg_query_wait_time_ms))/SUM(ws.total_query_wait_time_ms/ws.avg_query_wait_time_ms)))*1,2) stdev_query_wait_time,
    ROUND(CONVERT(float, SUM(ws.total_query_wait_time_ms))*1,2) total_query_wait_time,
    CAST(ROUND(SUM(ws.total_query_wait_time_ms/ws.avg_query_wait_time_ms),0) AS BIGINT) count_executions
FROM sys.query_store_wait_stats ws
    JOIN sys.query_store_runtime_stats_interval itvl ON itvl.runtime_stats_interval_id = ws.runtime_stats_interval_id
WHERE NOT (itvl.start_time > @interval_end_time OR itvl.end_time < @interval_start_time)
GROUP BY ws.wait_category, wait_category_desc
ORDER BY total_query_wait_time DESC 