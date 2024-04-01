SELECT
    p.is_forced_plan,
    p.query_plan
FROM
    sys.query_store_plan p
WHERE
    p.query_id = @query_id
    AND p.plan_id = @plan_id 