import requests
import pandas as pd
from bs4 import BeautifulSoup

def extract_data():
    # Example: Extract table data from Wikipedia
    url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'class':'wikitable'})
    df = pd.read_html(str(table))[0]
    return df