from vanna import base
from vanna.types import TrainingPlan, TrainingPlanItem


class MyVannaBase(base.VannaBase):
    def connect_to_bigquery(self, project_id: str, access_token: str | None = None):
        from google.cloud import bigquery
        from google.oauth2.credentials import Credentials as OAuth2Credentials

        conn = bigquery.Client(
            project=project_id,
            credentials=OAuth2Credentials(token=access_token) if access_token else None,
        )

        def run_sql_bigquery(sql: str):
            if conn:
                job = conn.query(sql)
                df = job.result().to_dataframe()
                return df
            return None

        def dry_run_sql_bigquery(sql: str):
            if conn:
                job = conn.query(
                    query=sql, job_config=bigquery.QueryJobConfig(dry_run=True)
                )
                return job.total_bytes_processed
            return None

        self.dialect = "BigQuery SQL"
        self.run_sql_is_set = True
        self.run_sql = run_sql_bigquery
        self.dry_run_sql = dry_run_sql_bigquery

    # def get_training_plan_bigquery(
    #     self,
    #     filter_databases: list[str] | None = None,
    #     filter_schemas: list[str] | None = None,
    #     include_information_schema: bool = False,
    #     use_historical_queries: bool = True,
    # ) -> TrainingPlan:
    #     plan = TrainingPlan([])

    #     if self.run_sql_is_set is False:
    #         raise ImproperlyConfigured("Please connect to a database first.")

    #     if use_historical_queries:
    #         try:
    #             print("Trying query history")
    #             df_history = self.run_sql(
    #                 """ select * from table(information_schema.query_history(result_limit => 5000)) order by start_time"""
    #             )

    #             df_history_filtered = df_history.query("ROWS_PRODUCED > 1")
    #             if filter_databases is not None:
    #                 mask = (
    #                     df_history_filtered["QUERY_TEXT"]
    #                     .str.lower()
    #                     .apply(
    #                         lambda x: any(
    #                             s in x for s in [s.lower() for s in filter_databases]
    #                         )
    #                     )
    #                 )
    #                 df_history_filtered = df_history_filtered[mask]

    #             if filter_schemas is not None:
    #                 mask = (
    #                     df_history_filtered["QUERY_TEXT"]
    #                     .str.lower()
    #                     .apply(
    #                         lambda x: any(
    #                             s in x for s in [s.lower() for s in filter_schemas]
    #                         )
    #                     )
    #                 )
    #                 df_history_filtered = df_history_filtered[mask]

    #             if len(df_history_filtered) > 10:
    #                 df_history_filtered = df_history_filtered.sample(10)

    #             for query in df_history_filtered["QUERY_TEXT"].unique().tolist():
    #                 plan._plan.append(
    #                     TrainingPlanItem(
    #                         item_type=TrainingPlanItem.ITEM_TYPE_SQL,
    #                         item_group="",
    #                         item_name=self.generate_question(query),
    #                         item_value=query,
    #                     )
    #                 )

    #         except Exception as e:
    #             print(e)

    #     databases = self._get_databases()

    #     for database in databases:
    #         if filter_databases is not None and database not in filter_databases:
    #             continue

    #         try:
    #             df_tables = self._get_information_schema_tables(database=database)

    #             print(f"Trying INFORMATION_SCHEMA.COLUMNS for {database}")
    #             df_columns = self.run_sql(
    #                 f"SELECT * FROM {database}.INFORMATION_SCHEMA.COLUMNS"
    #             )

    #             for schema in df_tables["TABLE_SCHEMA"].unique().tolist():
    #                 if filter_schemas is not None and schema not in filter_schemas:
    #                     continue

    #                 if (
    #                     not include_information_schema
    #                     and schema == "INFORMATION_SCHEMA"
    #                 ):
    #                     continue

    #                 df_columns_filtered_to_schema = df_columns.query(
    #                     f"TABLE_SCHEMA == '{schema}'"
    #                 )

    #                 try:
    #                     tables = (
    #                         df_columns_filtered_to_schema["TABLE_NAME"]
    #                         .unique()
    #                         .tolist()
    #                     )

    #                     for table in tables:
    #                         df_columns_filtered_to_table = (
    #                             df_columns_filtered_to_schema.query(
    #                                 f"TABLE_NAME == '{table}'"
    #                             )
    #                         )
    #                         doc = f"The following columns are in the {table} table in the {database} database:\n\n"
    #                         doc += df_columns_filtered_to_table[
    #                             [
    #                                 "TABLE_CATALOG",
    #                                 "TABLE_SCHEMA",
    #                                 "TABLE_NAME",
    #                                 "COLUMN_NAME",
    #                                 "DATA_TYPE",
    #                                 "COMMENT",
    #                             ]
    #                         ].to_markdown()

    #                         plan._plan.append(
    #                             TrainingPlanItem(
    #                                 item_type=TrainingPlanItem.ITEM_TYPE_IS,
    #                                 item_group=f"{database}.{schema}",
    #                                 item_name=table,
    #                                 item_value=doc,
    #                             )
    #                         )

    #                 except Exception as e:
    #                     print(e)
    #                     pass
    #         except Exception as e:
    #             print(e)

    #     return plan
