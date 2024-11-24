import io

import seaborn
import matplotlib.pyplot as plt
import hvac
import telegram
from airflow.hooks.base import BaseHook

from common.ssl import verify


def create_price_plot(data, item_name):
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
    # Get Vault connection from Airflow
    conn = BaseHook.get_connection("vault")

    # Initialize Vault client
    vault_client = hvac.Client(url=conn.host, token=conn.password, verify=verify)

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
