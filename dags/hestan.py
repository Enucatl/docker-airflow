from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

default_args = {
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "email_on_failure": True,
}


venv_cache_path = "/opt/airflow/venv"

with DAG(
    "hestan",
    default_args=default_args,
    description="Scrape Hestan product page for prices",
    schedule="@hourly",
    catchup=False,
) as dag:

    @task.virtualenv(
        task_id="scrape",
        requirements=[
            "beautifulsoup4",
            "pandas",
        ],
        venv_cache_path=venv_cache_path,
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
        venv_cache_path=venv_cache_path,
    )
    def analyze(table_name):
        import io
        import asyncio

        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import telegram

        def create_price_plot(data, item_name):
            import seaborn
            import matplotlib.pyplot as plt

            # Set the style
            seaborn.set_style("whitegrid")
            plt.figure(figsize=(10, 6))

            # Create the line plot
            seaborn.lineplot(data=data, x="timestamp", y="price")

            # Customize the plot
            plt.title(f"Price Trend for {item_name} - Last 2 Weeks")
            plt.xlabel("Date")
            plt.ylabel("Price (GBP)")
            plt.xticks(rotation=45)
            plt.tight_layout()

            # Save the plot to a bytes buffer
            buf = io.BytesIO()
            plt.savefig(buf, format="png")
            buf.seek(0)
            plt.close()

            return buf

        def get_vault_credentials():
            from airflow.hooks.base import BaseHook
            import hvac

            # Get Vault connection from Airflow
            conn = BaseHook.get_connection("vault")
            verify = "/opt/airflow/certs/puppet_ca.pem"

            # Initialize Vault client
            vault_client = hvac.Client(
                url=conn.host, token=conn.password, verify=verify
            )

            # Get Telegram credentials from Vault
            telegram_secrets = vault_client.secrets.kv.v2.read_secret_version(
                path="enucatl_bot", mount_point="secret"
            )

            return {
                "token": telegram_secrets["data"]["data"]["token"],
                "chat_id": telegram_secrets["data"]["data"]["chat"],
            }

        async def send_telegram_message(message, plot_buffer=None):
            credentials = get_vault_credentials()
            bot = telegram.Bot(token=credentials["token"])

            # Await the async operations
            await bot.send_message(
                chat_id=credentials["chat_id"], text=message, parse_mode="Markdown"
            )
            if plot_buffer:
                await bot.send_photo(chat_id=credentials["chat_id"], photo=plot_buffer)

        # Get Postgres connection from Airflow
        pg_hook = PostgresHook(postgres_conn_id="data")
        engine = pg_hook.get_sqlalchemy_engine()
        df = pd.read_sql(table_name, engine)
        for key in df["name"].unique():
            data = df[df["name"] == key]
            latest_price = data[data["timestamp"] == data["timestamp"].max()][
                "price"
            ].max()
            latest_timestamp = data["timestamp"].max()
            avg_price = data["price"].mean()
            two_weeks_ago = latest_timestamp - pd.Timedelta(days=14)
            recent_data = data[data["timestamp"] >= two_weeks_ago]
            plot_buffer = create_price_plot(recent_data, key)
            if latest_price < avg_price:
                message = (
                    f"Alert for {key}!\n"
                    f"Latest price ({latest_price:.2f}) is below "
                    f"average ({avg_price:.2f})\n"
                    f"Timestamp: {latest_timestamp.isoformat()}\n"
                    f"Below is the price trend for the last two weeks.\n\n"
                    f"```\n{recent_data.to_string()}\n```"
                )

                asyncio.run(send_telegram_message(message, plot_buffer))

    table = scrape()
    analyze(table)
