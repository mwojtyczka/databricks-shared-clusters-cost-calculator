from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql


class DBSQLHandler:
    def __init__(self, w: WorkspaceClient, data_source_id):
        self.w = w
        self.data_source_id = data_source_id

    def _delete_alert_if_exists(self, name: str):
        alert_id = None
        for alert in self.w.alerts.list():

            if alert.name == name:
                print(f"found alert: {name}")
                alert_id = alert.id
                break

        if alert_id:
            print(f"deleting alert: {name}")
            self.w.alerts.delete(alert_id=alert_id)

    def _delete_query_if_exists(self, name: str):
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
            data_source_id=self.data_source_id,
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

    def create_query_and_alert(
        self, query_name, query_body, query_description, alert_name
    ):
        query = self._create_query(query_name, query_body, query_description)
        self._create_alert(alert_name, query.id)

    def delete_query_and_alert(self, query_name, alert_name):
        self._delete_alert_if_exists(alert_name)
        self._delete_query_if_exists(query_name)
