from databricks.sdk.service import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import Dashboard
from databricks.sdk.service.dashboards import PublishedDashboard


class SqlObjectsHandler:
    def __init__(self, w: WorkspaceClient):
        self.w = w

    def create_query_and_alert(
        self, query_name, query_body, query_description, alert_name
    ):
        query = self._create_query(query_name, query_body, query_description)
        self._create_alert(alert_name, query.id)

    def delete_query_and_alert(self, query_name, alert_name):
        self._delete_alert(alert_name)
        self._delete_query(query_name)

    def delete_dashboard(self, name: str):
        dashboard_id = None
        for dashboard in self.w.lakeview.list():
            if dashboard.display_name == name:
                print(f"found dashboard: {name}")
                dashboard_id = dashboard.dashboard_id
                break

        if dashboard_id:
            print(f"deleting dashboard: {name}")
            self.w.lakeview.trash(dashboard_id=dashboard_id)

    def create_dashboard(
        self,
        name: str,
        serialized_dashboard: str,
        parent_path: str | None = None,
        warehouse_id: str | None = None,
    ) -> Dashboard:
        print(f"creating dashboard: {name}")
        return self.w.lakeview.create(
            display_name=name,
            serialized_dashboard=serialized_dashboard,
            parent_path=parent_path,
            warehouse_id=warehouse_id,
        )

    def publish_dashboard(
        self, dashboard_id: str, warehouse_id: str | None = None
    ) -> PublishedDashboard:
        print(f"publishing dashboard: {dashboard_id}")
        return self.w.lakeview.publish(
            dashboard_id=dashboard_id, warehouse_id=warehouse_id
        )

    def _delete_alert(self, name: str):
        alert_id = None
        for alert in self.w.alerts.list():

            if alert.name == name:
                print(f"found alert: {name}")
                alert_id = alert.id
                break

        if alert_id:
            print(f"deleting alert: {name}")
            self.w.alerts.delete(alert_id=alert_id)

    def _delete_query(self, name: str):
        query_id = None
        for query in self.w.queries.list():
            if query.name == name:
                print(f"found query: {name}")
                query_id = query.id
                break

        if query_id:
            print(f"deleting query: {name}")
            self.w.queries.delete(query_id=query_id)

    def _create_query(self, name, query_body, description):
        print(f"creating query: {name}")
        return self.w.queries.create(
            name=name,
            description=description,
            query=query_body,
        )

    def _create_alert(self, name, query_id):
        print(f"creating alert: {name}")
        return self.w.alerts.create(
            options=sql.AlertOptions(column="1", op=">", value="0"),
            name=name,
            query_id=query_id,
        )
