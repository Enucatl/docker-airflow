from airflow import DAG
from airflow.decorators import task

from common.default_args import default_args, venv_cache_path

venv_cache_path = "/opt/airflow/venv"

with DAG(
    "hestan",
    default_args=default_args,
    description="Scrape Hestan product page for prices",
    schedule="@hourly",
) as dag:

    @task.virtualenv(
        task_id="scrape",
        requirements=[
            "beautifulsoup4",
            "pandas",
        ],
    )
    def scrape():
        import pandas as pd
        from bs4 import BeautifulSoup
        from playwright.sync_api import sync_playwright
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        # Scrape the webpage
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        now = pd.Timestamp.now()
        product_infos = []

        # URL to scrape
        urls = {
            "small": "https://hestanculinary.co/products/nanobond-induction-skillet-fry-pan?variant=43156722647296",
            "medium": "https://hestanculinary.co/products/nanobond-induction-skillet-fry-pan?variant=43156722680064",
        }

        with sync_playwright() as p:
            for name, url in urls.items():
                # Launch browser
                browser = p.chromium.launch(headless=True)
                page = browser.new_page(user_agent=user_agent)

                # Go to URL and wait for content to load
                page.goto(url)
                class_ = "product__price"
                page.wait_for_selector(f".{class_}", state="visible", timeout=2000)
                content = page.content()

                browser.close()

                # Parse the HTML content
                soup = BeautifulSoup(content, "html.parser")

                price = (
                    soup.find("span", class_=class_).text.strip()
                    if soup.find("span", class_=class_)
                    else ""
                )
                currency = price[0]
                price = float(price[1:])
                # Extract the main content (you might need to adjust these selectors based on the actual page structure)
                product_info = {
                    "timestamp": now,
                    "name": name,
                    "price": price,
                    "currency": currency,
                }
                product_infos.append(product_info)

        # Get Postgres connection from Airflow
        pg_hook = PostgresHook(postgres_conn_id="data")
        engine = pg_hook.get_sqlalchemy_engine()
        table_name = "hestan_prices"
        df = pd.DataFrame(product_infos)
        print(df)
        df.to_sql(table_name, engine, if_exists="append", index=False)
        return table_name

    @task.virtualenv(
        task_id="analyze",
        requirements=[
            "beautifulsoup4",
            "hvac",
            "matplotlib",
            "pandas",
            "python-telegram-bot",
            "seaborn",
        ],
    )
    def analyze(table_name):
        import asyncio

        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        from hestan.functions import create_price_plot, send_telegram_message

        messages_table = "hestan_messages"
        pg_hook = PostgresHook(postgres_conn_id="data")
        engine = pg_hook.get_sqlalchemy_engine()
        df = pd.read_sql(table_name, engine)
        messages = pd.read_sql(messages_table, engine)
        latest_timestamp = df["timestamp"].max()
        two_weeks_ago = latest_timestamp - pd.Timedelta(days=14)
        recent_messages = messages[messages["timestamp"] >= two_weeks_ago]
        for key in df["name"].unique():
            data = df[df["name"] == key]
            latest_price = data[data["timestamp"] == data["timestamp"].max()][
                "price"
            ].max()
            recent_data = data[data["timestamp"] >= two_weeks_ago]
            merged_data = pd.merge(
                recent_data,
                recent_messages,
                on=["name"],
                how="left",
                suffixes=("", "_sent"),
            )
            message_data = merged_data[
                merged_data["timestamp_sent"].isnull()
                | (
                    merged_data["timestamp_sent"].notnull()
                    & (merged_data["price"] < merged_data["price_sent"])
                    & (merged_data["timestamp"] >= merged_data["timestamp_sent"])
                )
            ]
            if not message_data.empty:
                message = (
                    f"{key}: price {latest_price:.2f}"
                    f"```\n{recent_data[['timestamp', 'price']].to_string()}\n```"
                )
                plot_buffer = create_price_plot(recent_data, key)
                asyncio.run(send_telegram_message(message, plot_buffer))
                sent = pd.DataFrame(
                    {
                        "timestamp": [latest_timestamp],
                        "price": [latest_price],
                        "name": [key],
                    }
                )
                sent.to_sql(messages_table, engine, if_exists="append", index=False)
        return messages_table

    table = scrape()
    analyze(table)
