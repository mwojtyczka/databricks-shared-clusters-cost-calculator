def _get_base_alert_query_body(catalog_and_schema: str):
    # limit alerting to the current month using previous day data
    return f"""
        WITH monthly_costs AS (
            SELECT
              ui.organizational_entity_value AS department,
              cad.currency_code,
              CAST(DATE_TRUNC('month', cad.billing_date) AS DATE) as month,
              SUM(cad.dbu_cost) AS total_dbu_cost,
              SUM(cad.cloud_cost) AS total_cloud_cost
            FROM {catalog_and_schema}.cost_agg_day cad
            INNER JOIN {catalog_and_schema}.user_info ui
              ON ui.user_name = cad.user_name
            WHERE
              ui.organizational_entity_name = 'department'
            GROUP BY
              ui.organizational_entity_value,
              cad.currency_code,
              DATE_TRUNC('month', cad.billing_date)
        ),
        budget_info AS (
            SELECT
              organizational_entity_value AS department,
              effective_start_date,
              COALESCE(effective_end_date, CURRENT_DATE) AS effective_end_date,
              dbu_cost_limit,
              cloud_cost_limit,
              currency_code
            FROM {catalog_and_schema}.budget
            WHERE
              organizational_entity_name = 'department'
        ), cost_report AS (
            SELECT
              mc.department,
              mc.month,
              mc.total_dbu_cost,
              bi.dbu_cost_limit,
              mc.total_cloud_cost,
              bi.cloud_cost_limit,
              CASE
                WHEN mc.total_dbu_cost IS NULL OR bi.dbu_cost_limit IS NULL THEN NULL
                WHEN mc.total_dbu_cost <= bi.dbu_cost_limit THEN false
                ELSE true
              END AS dbu_cost_over_budget,
              CASE
                WHEN mc.total_cloud_cost IS NULL OR bi.cloud_cost_limit IS NULL THEN NULL
                WHEN mc.total_cloud_cost <= bi.cloud_cost_limit THEN false
                ELSE true
              END AS cloud_cost_over_budget
            FROM monthly_costs mc
            INNER JOIN budget_info bi
              ON mc.department = bi.department
                AND mc.currency_code = bi.currency_code
                AND mc.month BETWEEN bi.effective_start_date AND bi.effective_end_date
            WHERE CAST(DATE_TRUNC('month', DATE_SUB(DATE_TRUNC('day', CURRENT_DATE), 1)) AS DATE) = mc.month
            ORDER BY
              mc.department,
              mc.month
        )
        SELECT COUNT(1) AS number_of_budget_violations
        FROM cost_report
    """


def get_dbu_cost_alert_query_body(catalog_and_schema: str):
    """
    Get query body for the dbu cost alert.
    @param catalog_and_schema: catalog and schema to use
    @return:
    """
    return (
        _get_base_alert_query_body(catalog_and_schema)
        + " WHERE dbu_cost_over_budget = true"
    )


def get_cloud_cost_alert_query_body(catalog_and_schema: str):
    """
    Get query body for the cloud cost alert.
    @param catalog_and_schema: catalog and schema to use
    @return:
    """
    return (
        _get_base_alert_query_body(catalog_and_schema)
        + " WHERE dbu_cost_over_budget = true"
    )
