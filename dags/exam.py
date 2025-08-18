from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from common.defaults import default_args


with DAG(
    "exam",
    default_args=default_args,
    description="Scrape exam dates out of the Kanton Zurich website",
    schedule="@daily",
    start_date=datetime(2025, 8, 17),
) as dag:

    @task.virtualenv(
        task_id="login",
        requirements=[
            "beautifulsoup4",
            "pandas",
        ],
    )
    def scrape(logical_date):
        import pandas as pd
        from bs4 import BeautifulSoup
        from playwright.sync_api import sync_playwright
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from airflow.hooks.base import BaseHook

        # # Scrape the webpage
        # user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        # now = pd.Timestamp.now()

        # with sync_playwright() as p:
        #     # Launch browser
        #     browser = p.chromium.launch(headless=True)
        #     page = browser.new_page(user_agent=user_agent)

        #     connection = BaseHook.get_connection("kanton_zurich")
        #     username_selector = "birthday"
        #     password_selector = "candidateId"
        #     try:
        #         # Go to URL and wait for content to load
        #         page.goto(connection.host)
        #         page.locator(username_selector).fill(connection.username)
        #         page.locator(password_selector).fill(connection.password)
        #         page.wait_for_load_state("networkidle", timeout=30000)
        #         content = page.content()
        #         print(content)
        #     finally:
        #         browser.close()

        # Get Postgres connection from Airflow
        pg_hook = PostgresHook(postgres_conn_id="data")
        engine = pg_hook.get_sqlalchemy_engine()
        table_name = "exam_appointment"
        return table_name

    # @task.virtualenv(
    #     task_id="analyze",
    #     requirements=[
    #         "beautifulsoup4",
    #         "hvac",
    #         "matplotlib",
    #         "pandas",
    #         "python-telegram-bot",
    #         "seaborn",
    #     ],
    # )
    # def analyze(table_name):
    #     import asyncio

    #     import pandas as pd
    #     from airflow.providers.postgres.hooks.postgres import PostgresHook

    #     from hestan.functions import create_price_plot, send_telegram_message

    #     messages_table = "hestan_messages"
    #     pg_hook = PostgresHook(postgres_conn_id="data")
    #     engine = pg_hook.get_sqlalchemy_engine()
    #     df = pd.read_sql(table_name, engine)
    #     messages = pd.read_sql(messages_table, engine)
    #     latest_timestamp = df["timestamp"].max()
    #     recent_cutoff_date = latest_timestamp - pd.Timedelta(days=14)
    #     recent_messages = messages[messages["timestamp"] >= recent_cutoff_date]
    #     for key in df["name"].unique():
    #         data = df[df["name"] == key]
    #         latest_price = data[data["timestamp"] == data["timestamp"].max()][
    #             "price"
    #         ].max()
    #         recent_data = data[data["timestamp"] >= recent_cutoff_date]
    #         merged_data = pd.merge(
    #             recent_data,
    #             recent_messages,
    #             on=["name"],
    #             how="left",
    #             suffixes=("", "_sent"),
    #         )
    #         message_data = merged_data[
    #             merged_data["timestamp_sent"].isnull()
    #             | (
    #                 merged_data["timestamp_sent"].notnull()
    #                 & (merged_data["price"] < merged_data["price_sent"])
    #                 & (merged_data["timestamp"] >= merged_data["timestamp_sent"])
    #             )
    #         ]
    #         if not message_data.empty:
    #             message = (
    #                 f"{key}: price {latest_price:.2f}"
    #                 f"```\n{recent_data[['timestamp', 'price']].tail(5).to_string()}\n```"
    #             )
    #             plot_buffer = create_price_plot(recent_data, key)
    #             asyncio.run(send_telegram_message(message, plot_buffer))
    #             sent = pd.DataFrame({
    #                 "timestamp": [latest_timestamp],
    #                 "price": [latest_price],
    #                 "name": [key],
    #             })
    #             sent.to_sql(messages_table, engine, if_exists="append", index=False)
    #     return messages_table

    table = scrape()
    # analyze(table)
