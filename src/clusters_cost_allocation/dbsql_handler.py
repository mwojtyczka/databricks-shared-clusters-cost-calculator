import logging
from databricks.sdk.service import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import Dashboard
from databricks.sdk.service.dashboards import PublishedDashboard

logger = logging.getLogger(__name__)


class SqlObjectsHandler:
    def __init__(self, w: WorkspaceClient):
        self.w = w

    def create_query_and_alert(
        self, query_name: str, query_body: str, query_description: str, alert_name: str
    ):
        """
        Create alert and associated query.
        @param query_name: query name
        @param query_body: query body
        @param query_description: query description
        @param alert_name: alert name
        """
        query = self._create_query(query_name, query_body, query_description)
        self._create_alert(alert_name, query.id)

    def delete_query_and_alert(self, query_name: str, alert_name: str):
        """
        Delete alert and associated query.
        @param query_name: query name
        @param alert_name: alert name
        """
        self._delete_alert(alert_name)
        self._delete_query(query_name)

    def delete_dashboard(self, name: str):
        """
        Delete dashboard
        @param name: dashboard name
        """
        dashboard_id = None
        for dashboard in self.w.lakeview.list():
            if dashboard.display_name == name:
                logger.info(f"found dashboard: {name}")
                dashboard_id = dashboard.dashboard_id
                break

        if dashboard_id:
            logger.info(f"deleting dashboard: {name}")
            self.w.lakeview.trash(dashboard_id=dashboard_id)

    def create_dashboard(
        self,
        name: str,
        serialized_dashboard: str,
        parent_path: str | None = None,
        warehouse_id: str | None = None,
    ) -> Dashboard:
        """
            Create dashboard.
            @param name: dashboard name
            @param serialized_dashboard: The contents of the dashboard in serialized string form.
            @param parent_path: The workspace path of the folder containing the dashboard. Includes leading slash and no
        trailing slash
            @param warehouse_id: The warehouse ID used to run the dashboard
            @return:
        """
        logger.info(f"creating dashboard: {name}")
        return self.w.lakeview.create(
            display_name=name,
            serialized_dashboard=serialized_dashboard,
            parent_path=parent_path,
            warehouse_id=warehouse_id,
        )

    def publish_dashboard(
        self, dashboard_id: str, warehouse_id: str | None = None
    ) -> PublishedDashboard:
        """
        Publish dashboard
        @param dashboard_id: dashboard id
        @param warehouse_id: The warehouse ID used to run the dashboard
        @return:
        """
        logger.info(f"publishing dashboard: {dashboard_id}")
        return self.w.lakeview.publish(
            dashboard_id=dashboard_id, warehouse_id=warehouse_id
        )

    def _delete_alert(self, name: str):
        alert_id = None
        for alert in self.w.alerts.list():
            if alert.display_name == name:
                logger.info(f"found alert: {name}")
                alert_id = alert.id
                break

        if alert_id:
            logger.info(f"deleting alert: {name}")
            self.w.alerts.delete(id=alert_id)

    def _delete_query(self, name: str):
        query_id = None
        for query in self.w.queries.list():
            if query.display_name == name:
                logger.info(f"found query: {name}")
                query_id = query.id
                break

        if query_id:
            logger.info(f"deleting query: {name}")
            self.w.queries.delete(id=query_id)

    def _create_query(self, name: str, query_body: str, description: str):
        logger.info(f"creating query: {name}")
        return self.w.queries.create(
            query=sql.CreateQueryRequestQuery(
                display_name=name, description=description, query_text=query_body
            )
        )

    def _create_alert(self, name: str, query_id: str):
        logger.info(f"creating alert: {name}")
        return self.w.alerts.create(
            alert=sql.CreateAlertRequestAlert(
                condition=sql.AlertCondition(
                    operand=sql.AlertConditionOperand(
                        column=sql.AlertOperandColumn(name="1")
                    ),
                    op=sql.AlertOperator.GREATER_THAN,
                    threshold=sql.AlertConditionThreshold(
                        value=sql.AlertOperandValue(double_value=0)
                    ),
                ),
                display_name=name,
                query_id=query_id,
            )
        )
