import os

from langchain_openai import OpenAIEmbeddings
from openai import OpenAI
from vanna.base import VannaBase
from vanna.exceptions import ImproperlyConfigured
from vanna.openai.openai_chat import OpenAI_Chat
from vanna.pgvector import PG_VectorStore
from vanna.types import TrainingPlan, TrainingPlanItem


class MyVannaBase(VannaBase):
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

    def get_training_plan_bigquery(
        self,
        location: str,
        resource_id: str,
    ) -> TrainingPlan:
        plan = TrainingPlan([])

        if self.run_sql_is_set is False:
            raise ImproperlyConfigured("Please connect to BigQuery first")

        # if use_historical_queries:
        #     try:
        #         print("Trying query history")
        #         df_history = self.run_sql(
        #             """ select * from table(information_schema.query_history(result_limit => 5000)) order by start_time"""
        #         )

        #         df_history_filtered = df_history.query("ROWS_PRODUCED > 1")
        #         if filter_databases is not None:
        #             mask = (
        #                 df_history_filtered["QUERY_TEXT"]
        #                 .str.lower()
        #                 .apply(
        #                     lambda x: any(
        #                         s in x for s in [s.lower() for s in filter_databases]
        #                     )
        #                 )
        #             )
        #             df_history_filtered = df_history_filtered[mask]

        #         if filter_schemas is not None:
        #             mask = (
        #                 df_history_filtered["QUERY_TEXT"]
        #                 .str.lower()
        #                 .apply(
        #                     lambda x: any(
        #                         s in x for s in [s.lower() for s in filter_schemas]
        #                     )
        #                 )
        #             )
        #             df_history_filtered = df_history_filtered[mask]

        #         if len(df_history_filtered) > 10:
        #             df_history_filtered = df_history_filtered.sample(10)

        #         for query in df_history_filtered["QUERY_TEXT"].unique().tolist():
        #             plan._plan.append(
        #                 TrainingPlanItem(
        #                     item_type=TrainingPlanItem.ITEM_TYPE_SQL,
        #                     item_group="",
        #                     item_name=self.generate_question(query),
        #                     item_value=query,
        #                 )
        #             )

        #     except Exception as e:
        #         print(e)

        project_id, dataset_id, table_id = resource_id.split(".")

        df_tables = self.run_sql(
            f"SELECT DDL FROM `{project_id}.region-{location}`.INFORMATION_SCHEMA.TABLES WHERE table_schema='{dataset_id}' AND table_name='{table_id}'"
        )
        ddl = df_tables["DDL"].iloc[0]

        plan._plan.append(
            TrainingPlanItem(
                item_type=TrainingPlanItem.ITEM_TYPE_IS,
                item_group=f"{project_id}.{dataset_id}",
                item_name=table_id,
                item_value=ddl,
            )
        )

        df_table_options = self.run_sql(
            f"SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, OPTION_NAME, OPTION_TYPE FROM `{project_id}.region-{location}`.INFORMATION_SCHEMA.TABLE_OPTIONS WHERE TABLE_SCHEMA='{dataset_id}' AND TABLE_NAME='{table_id}'"
        )
        tbl_doc = f"INFORMATION_SCHEMA.TABLE_OPTIONS of `{project_id}.{dataset_id}.{table_id}`:\n\n"
        tbl_doc += df_table_options[
            [
                "TABLE_CATALOG",
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "OPTION_NAME",
                "OPTION_TYPE",
            ]
        ].to_markdown()

        plan._plan.append(
            TrainingPlanItem(
                item_type=TrainingPlanItem.ITEM_TYPE_IS,
                item_group=f"{project_id}.{dataset_id}",
                item_name=table_id,
                item_value=tbl_doc,
            )
        )

        df_column_field_paths = self.run_sql(
            f"SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, FIELD_PATH, DATA_TYPE, DESCRIPTION FROM `{project_id}.region-{location}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS WHERE TABLE_SCHEMA='{dataset_id}' AND TABLE_NAME='{table_id}'"
        )
        col_doc = f"INFORMATION_SCHEMA.COLUMN_FIELD_PATHS of `{project_id}.{dataset_id}.{table_id}`:\n\n"
        col_doc += df_column_field_paths[
            [
                "TABLE_CATALOG",
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "COLUMN_NAME",
                "FIELD_PATH",
                "DATA_TYPE",
                "DESCRIPTION",
            ]
        ].to_markdown()

        plan._plan.append(
            TrainingPlanItem(
                item_type=TrainingPlanItem.ITEM_TYPE_IS,
                item_group=f"{project_id}.{dataset_id}",
                item_name=table_id,
                item_value=col_doc,
            )
        )

        return plan


class VannaAgent(MyVannaBase, PG_VectorStore, OpenAI_Chat):
    def __init__(self) -> None:
        PG_VectorStore.__init__(
            self,
            config={
                "connection_string": os.getenv("PGVECTOR_URL"),
                "embedding_function": OpenAIEmbeddings(model="text-embedding-3-large"),
            },
        )
        OpenAI_Chat.__init__(
            self,
            client=OpenAI(),
            config={
                "model": "gpt-4o",
            },
        )
