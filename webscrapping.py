from bs4 import BeautifulSoup
import requests

html = requests.get("https://www.climatempo.com.br/").content
soup = BeautifulSoup(html, 'html.parser')
print(soup.prettify())

temperatura = soup.find("span", class_="_block _margin-b-5 -gray")

print(temperatura.string)
