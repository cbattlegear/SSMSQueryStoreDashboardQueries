WITH 
    bucketizer as 
    (
        SELECT
            rs.plan_id as plan_id,
            rs.execution_type as execution_type,
            SUM(rs.count_executions) count_executions,
            DATEADD(mi, ((DATEDIFF(mi, 0, rs.last_execution_time))),0 ) as bucket_start,
            DATEADD(mi, (1 + (DATEDIFF(mi, 0, rs.last_execution_time))), 0) as bucket_end,
            ROUND(CONVERT(float, SUM(rs.avg_duration*rs.count_executions))/NULLIF(SUM(rs.count_executions), 0)*0.001,2) as avg_duration,
            ROUND(CONVERT(float, MAX(rs.max_duration))*0.001,2) as max_duration,
            ROUND(CONVERT(float, MIN(rs.min_duration))*0.001,2) as min_duration,
            ROUND(CONVERT(float, SQRT( SUM(rs.stdev_duration*rs.stdev_duration*rs.count_executions)/NULLIF(SUM(rs.count_executions), 0)))*0.001,2) as stdev_duration,
            ISNULL(ROUND(CONVERT(float, (SQRT( SUM(rs.stdev_duration*rs.stdev_duration*rs.count_executions)/NULLIF(SUM(rs.count_executions), 0))*SUM(rs.count_executions)) / NULLIF(SUM(rs.avg_duration*rs.count_executions), 0)),2), 0) as variation_duration,
            ROUND(CONVERT(float, SUM(rs.avg_duration*rs.count_executions))*0.001,2) as total_duration
        FROM
            sys.query_store_runtime_stats rs
            JOIN sys.query_store_plan p ON p.plan_id = rs.plan_id
        WHERE
            p.query_id = @query_id
        AND NOT (rs.first_execution_time > @interval_end_time OR rs.last_execution_time < @interval_start_time)
        GROUP BY
            rs.plan_id,
            rs.execution_type,
            DATEDIFF(mi, 0, rs.last_execution_time)
    ),
    is_forced as
    (
        SELECT is_forced_plan, plan_id
          FROM sys.query_store_plan
    )
SELECT b.plan_id as plan_id,
    is_forced_plan,
    execution_type,
    count_executions,
    SWITCHOFFSET(bucket_start, DATEPART(tz, @interval_start_time)) AS bucket_start,
    SWITCHOFFSET(bucket_end, DATEPART(tz, @interval_start_time)) AS bucket_end,
    avg_duration,
    max_duration,
    min_duration,
    stdev_duration,
    variation_duration,
    total_duration
FROM bucketizer b
JOIN is_forced f ON f.plan_id = b.plan_id 