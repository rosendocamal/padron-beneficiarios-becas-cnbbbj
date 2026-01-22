import time

start = time.time()

print('[INICIANDO] EJECUTANDO SCRIPTS')

print('[SCRIPT_1] OBTENIENDO ENLACES DE DESCARGA DE LOS DATASETS')
exec(open("etl/01_get_download_links.py").read())
end_phase_1 = time.time()
print(f'PRIMER TIEMPO DE EJECUCIÓN: {round(((end_phase_1 - start) / 60), 2)} MIN.')
print('[SCRIPT_1] ENLACES DE DESCARGA DE LOS DATASETS OBTENIDOS')

print('[SCRIPT_2] PREPARANDO PARA LA DESCARGA DE LOS DATASETS')
exec(open("etl/02_download_files.py").read())
end_phase_2 = time.time()
print(f'SEGUNDO TIEMPO DE EJECUCIÓN: {round(((end_phase_2 - end_phase_1) / 60), 2)} MIN.')
print('[SCRIPT_2] DESCARGA DE LOS DATASETS HA FINALIZADO CON ÉXITO')

print('[SCRIPT_3] UNIFICANDO LOS DATASETS')
exec(open("etl/03_merging_datasets.py").read())
end_phase_3 = time.time()
print(f'TERCER TIEMPO DE EJECUCIÓN: {round(((end_phase_3 - end_phase_2) / 60), 2)} MIN.')
print('[SCRIPT_3] LOS DATASETS SE HA UNIFICADO')

print(f'TIEMPO TOTAL DE EJECUCIÓN: {round(((end_phase_3 - start) / 60), 2)} MIN.')
print('[COMPLETADO] EJECUCIÓN DE SCRIPTS FINALIZADOS')
