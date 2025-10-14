import requests
import pandas as pd
from bs4 import BeautifulSoup

def extract_data():
    """
    Extract data from a webpage and return a cleaned DataFrame.
    Example uses Wikipedia GDP table.
    """
    url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)"
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'class': 'wikitable'})
    df_list = pd.read_html(str(table), flavor='lxml')
    df = df_list[0]
    return df
