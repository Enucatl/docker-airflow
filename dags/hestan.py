from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

default_args = {
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


venv_cache_path = "/opt/airflow/venv"

with DAG(
    "hestan",
    default_args=default_args,
    description="Scrape Hestan product page for prices",
    schedule="0 12 * * *",
    catchup=False,
) as dag:

    @task.virtualenv(
        task_id="scrape_and_analyze",
        requirements=[
            "beautifulsoup4",
        ],
        venv_cache_path=venv_cache_path,
    )
    def scrape_and_analyze():
        from bs4 import BeautifulSoup
        from playwright.sync_api import sync_playwright

        # URL to scrape
        urls = {
            "small": "https://hestanculinary.co/products/nanobond-induction-skillet-fry-pan?variant=43156722647296",
            "medium": "https://hestanculinary.co/products/nanobond-induction-skillet-fry-pan?variant=43156722680064",
        }

        # Scrape the webpage
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

        product_infos = []
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
                    "name": name,
                    "price": price,
                    "currency": currency,
                }
                product_infos.append(product_info)
        return product_infos

    scrape_and_analyze_task = scrape_and_analyze()
