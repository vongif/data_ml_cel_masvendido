from bs4 import BeautifulSoup
import requests
from datetime import datetime
import pandas as pd

#def scrape_mercadolibre():
   
#    return data


site = 'https://www.mercadolibre.com.ar/mas-vendidos/MLA1055#home=top-sales-first-recommendations&c_id=/home/top-sales-first-recommendations&c_uid=91c57d96-dbec-44b5-a374-bad6413b9ba2'
result = requests.get(site)
content = result.text
soup = BeautifulSoup(content, 'lxml')



grillas1 = soup.find_all('div', class_= 'ui-recommendations-card ui-recommendations-card--vertical show-original-price __item')

grillas2 = soup.find_all('div', class_= 'ui-recommendations-card ui-recommendations-card--vertical __item')



puesto1 = []
puesto2 = []
puesto3 = []


for grilla in grillas1:
  puesto1.append(grilla.find('span', class_='ui-recommendations-card__pill').get_text())
  puesto2.append(grilla.find('a', class_='ui-recommendations-card__link').get_text())
  puesto3.append(grilla.find('div', class_='ui-recommendations-card__price-block').get_text(strip=True, separator= ' '))


for grilla in grillas2:
  puesto1.append(grilla.find('span', class_='ui-recommendations-card__pill').get_text())
  puesto2.append(grilla.find('a', class_='ui-recommendations-card__link').get_text())
  puesto3.append(grilla.find('div', class_='ui-recommendations-card__price-block').get_text(strip=True, separator= ' '))


dict = {'Puesto': puesto1, 'Producto': puesto2, 'Precio': puesto3}

posiciones = pd.DataFrame(dict)

fecha_actual = datetime.now().date()
posiciones['Fecha_Actual'] = fecha_actual

posiciones


posiciones['numero_inicio'] = posiciones['Puesto'].apply(lambda x: int(x.split('º')[0]))

df_ordenado = posiciones.sort_values(by='numero_inicio')

df_ordenado

