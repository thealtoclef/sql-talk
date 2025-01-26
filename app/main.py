import os
from typing import TYPE_CHECKING

import chainlit as cl
from agents.vanna import VannaAgent
from chainlit.data.sql_alchemy import SQLAlchemyDataLayer
from chainlit.input_widget import TextInput
from plotly.graph_objs._figure import Figure

if TYPE_CHECKING:
    import pandas as pd


@cl.data_layer
def get_data_layer():
    return SQLAlchemyDataLayer(conninfo=os.getenv("DATA_LAYER_URL"))


@cl.oauth_callback
def oauth_callback(
    provider_id: str,
    token: str,
    raw_user_data: dict[str, str],
    default_user: cl.User,
) -> cl.User | None:
    return default_user


@cl.on_chat_start
async def setup() -> None:
    app_user = cl.user_session.get("user")
    await cl.Message(
        content=f"Hello {app_user.identifier}. Please update settings to initialize the agent",
    ).send()

    settings = await cl.ChatSettings(
        [
            TextInput(
                id="access_token",
                label="Access Token",
                description="Enter your access token from `gcloud auth print-access-token`",
            ),
            TextInput(
                id="bigquery_project_id",
                label="BigQuery Execution Project",
                description="Enter the project where you want to execute the BigQuery queries",
                initial="cake-user-adhoc",
            ),
            TextInput(
                id="location",
                label="Location",
                description="Enter the location of the BigQuery project",
                initial="asia-southeast1",
            ),
            TextInput(
                id="resource_id",
                label="Resource ID",
                description="Enter the resource ID in format `project_id.dataset_id.table_id`",
            ),
        ]
    ).send()
    await setup_agent(settings)


@cl.on_settings_update
async def setup_agent(settings) -> None:
    setting_errors = []
    for setting in settings:
        if settings[setting] is None:
            setting_errors.append(f"Setting {setting} cannot be empty")
    cl.user_session.set("setting_errors", setting_errors)

    if not setting_errors:
        print("Setup agent with following settings: ", settings)
        vanna_agent = VannaAgent()
        vanna_agent.connect_to_bigquery(
            project_id=settings["bigquery_project_id"],
            access_token=settings["access_token"],
        )
        plan = vanna_agent.get_training_plan_bigquery(
            location=settings["location"],
            resource_id=settings["resource_id"],
        )
        vanna_agent.train(plan=plan)
        cl.user_session.set("vanna_agent", vanna_agent)
        await cl.Message(
            content="Agent is ready to use. Please enter a query to get started.",
            author="Vanna",
        ).send()


@cl.step(language="sql", name="Generate Query")
async def gen_query(vn: VannaAgent, human_query: str) -> str:
    sql_query = vn.generate_sql(question=human_query)

    return sql_query


@cl.step(name="Dry Run Query")
async def dry_run_query(vn: VannaAgent, sql: str) -> int:
    bytes_processed = vn.dry_run_sql(sql=sql)

    return bytes_processed


@cl.step(name="Execute Query")
async def execute_query(vn: VannaAgent, query: str) -> "pd.DataFrame":
    current_step = cl.context.current_step
    df = vn.run_sql(sql=query)
    current_step.output = df.head().to_markdown(index=False)

    return df


@cl.step(name="Plot", language="python")
async def plot(
    vn: VannaAgent, human_query: str, sql: str, df: "pd.DataFrame"
) -> Figure:
    current_step = cl.context.current_step
    plotly_code = vn.generate_plotly_code(question=human_query, sql=sql, df=df)
    fig = vn.get_plotly_figure(plotly_code=plotly_code, df=df)

    current_step.output = plotly_code
    return fig


@cl.step(type="run", name="Vanna")
async def chain(vn: VannaAgent, human_query: str) -> None:
    try:
        # Generate SQL query from the human query and dry run it to get the bytes processed
        sql_query = await gen_query(vn=vn, human_query=human_query)

        try:
            bytes_processed = await dry_run_query(vn=vn, sql=sql_query)
        except Exception as e:
            await cl.ErrorMessage(
                content=sql_query,
                author="Vanna",
            ).send()
            raise e

        # Ask the user to confirm if they want to continue with the execution
        res = await cl.AskActionMessage(
            content=f"Query will process {bytes_processed} bytes.",
            actions=[
                cl.Action(
                    name="continue", payload={"value": "continue"}, label="✅ Continue"
                ),
                cl.Action(
                    name="cancel", payload={"value": "cancel"}, label="❌ Cancel"
                ),
            ],
        ).send()
        if res and res.get("payload").get("value") != "continue":
            await cl.Message(
                content="Query execution cancelled by the user.",
            ).send()
            return

        # Execute the SQL query and plot the results
        df = await execute_query(vn=vn, query=sql_query)
        fig = await plot(vn=vn, human_query=human_query, sql=sql_query, df=df)
        elements = [cl.Plotly(name="chart", figure=fig, display="inline")]
        await cl.Message(content=human_query, elements=elements, author="Vanna").send()
    except Exception:
        await cl.ErrorMessage(
            content="An unexpected error occurred. Please review the output of the agent's steps for more details. If the issue persists, please contact support.",
            author="Vanna",
        ).send()


@cl.on_message
async def main(message: cl.Message) -> None:
    settings_errors = cl.user_session.get("setting_errors")
    if settings_errors:
        await cl.ErrorMessage(
            content="\n".join(settings_errors),
            author="Vanna",
        ).send()
        return

    vanna_agent = cl.user_session.get("vanna_agent")
    await chain(vn=vanna_agent, human_query=message.content)
