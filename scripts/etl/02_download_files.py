import requests, os

PATH = '../data/raw/'

print('[INICIANDO] Iniciando descarga de archivos')
with open(f'{PATH}datasets_links.txt', 'r') as download_links:
    links = download_links.readlines()
    
    periods = ['Q1', 'Q2', 'Q3', 'Q4']
    
    def new_dir(rute):
        try:
            os.mkdir(rute)
        except FileExistsError:
            pass

    new_dir(f'{PATH}datasets')
    
    os.makedirs(f'{PATH}../processed/', exist_ok=True)
    with open(f'{PATH}../processed/names_files.txt', 'w') as n_files:
        for period in periods:
            new_dir(f'{PATH}datasets/{period}')
    

        name_file = str()
        for link_p in links:
            link = link_p[:-1]
        
            name_file = link.upper().split('/')[-1].split('_')[:-1]
            name_path = f'{PATH}datasets/'
            for i in range(4):
                if str(i + 1) in name_file[-1]:
                    name_file.remove(name_file[-1])
                    name_file.append(f'{periods[i]}.csv')
                    name_file = '_'.join(name_file)
                    name_path += f'{periods[i]}/{name_file}'    
                    print(f'[{links.index(link_p) + 1:03d}/{len(links)}] Descargando archivo: {name_file}')

                    n_files.write(f'{name_file}\n')

            with requests.get(link, stream=True) as resource:
                resource.raise_for_status()

                with open(name_path, 'wb') as file_out:
                    for chunk in resource.iter_content(chunk_size=8192):
                        file_out.write(chunk)
                print(f'[{links.index(link_p) + 1:03d}/{len(links)}] Archivo descargado: {name_file}')
print('[COMPLETADO] Archivos descargados')
