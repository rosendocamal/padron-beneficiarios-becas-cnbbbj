import requests, os
from bs4 import BeautifulSoup

datasets_url = 'https://www.datos.gob.mx/dataset/programa_nacional_becas_bienestar_benito_juarez_2025_programa_s311'

def get_all_download_links(url):
    answer = requests.get(url)
    soup = BeautifulSoup(answer.text, 'html.parser')

    resources = soup.find_all('li', class_='resource-item')
    print(f'[INICIANDO] Se encontraron {len(resources)} archivos disponibles.')
    
    os.makedirs('../data/raw/', exist_ok=True)
    with open('../data/raw/datasets_links.txt', 'w') as file_links:
        for resource in resources:
            download_boton = resource.find('a', class_='btn btn-outline-primary')

            if download_boton:
                download_link = download_boton['href']
                name_dataset = resource.find('a', class_='text-black')['title']
            
                print(f'[{resources.index(resource) + 1:03d}/{len(resources)}] Obteniendo enlance de descarga de: "{name_dataset}"...')
                print(f'[{resources.index(resource) + 1:03d}/{len(resources)}] Enlance obtenido de: "{name_dataset}"')

                file_links.write(f'{download_link}\n')
    print('[COMPLETADO] Se han obtenido todos los enlances para descargar los archivos')
        

get_all_download_links(datasets_url)
