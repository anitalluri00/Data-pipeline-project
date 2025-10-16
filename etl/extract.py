import requests
import pandas as pd
from bs4 import BeautifulSoup

def extract_data():
    """
    Extract data from Wikipedia GDP table using browser-like headers.
    """
    url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/119.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", {"class": "wikitable"})
    if table is None:
        raise ValueError("Could not find the GDP table on the page!")
    df_list = pd.read_html(str(table), flavor="lxml")
    df = df_list[0]
    return df
