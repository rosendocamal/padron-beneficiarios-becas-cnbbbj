import sqlite3, os, time

start_time = time.time()

# Abrir dataset unificado y limpio
with open('../data/processed/S311_CNBBBJ_2025.csv', 'r') as dataset:
    os.makedirs('../data/db/', exist_ok=True)   
    
    # Generar o conectarse con la base de datos
    with sqlite3.connect('../data/db/S311_CNBBBJ_2025.db') as database:
        
        cursor = database.cursor()
        print('Database created and connected succesfully!')

        # Extracción de los nombres de los campos
        fields = dataset.readline().strip('\n').split(';')
        
        # Creación de la tabla de datos
        name_table = 'BENEFICIARIOS'

        fields_type = {
                'ID': 'INTEGER PRIMARY KEY AUTOINCREMENT',
                fields[0]: 'INTEGER',
                fields[1]: 'INTEGER',
                fields[2]: 'TEXT',
                fields[3]: 'INTEGER',
                fields[4]: 'TEXT',
                fields[5]: 'INTEGER',
                fields[6]: 'TEXT',
                fields[7]: 'REAL',
                fields[8]: 'TEXT'
                }
        
        insert_fields = [f'{name_field} {type_field}' for name_field, type_field in fields_type.items()]
        create_table_query = f'CREATE TABLE IF NOT EXISTS {name_table} ({','.join(insert_fields)})'   

        cursor.execute(create_table_query)
        database.commit()

        print(f'Table "{name_table}" created succesfully!')

        # Insertar los registros del dataset unificado y limpio al database
        print('Please wait... Inserting records...')
        for record in dataset:
            # Obtener los valores del registro del dataset
            data = record.strip('\n').split(';')
            values = (int(data[0]), int(data[1]), data[2], int(data[3]), data[4], int(data[5]), data[6], float(data[7]), data[8])

            # Insertar el registro a la tabla anteriormente creada
            insert_query = f'INSERT INTO {name_table} ({','.join(fields)})\nVALUES ({",".join(["?" for _ in range(9)])})'

            cursor.execute(insert_query, values)
        database.commit()
        print('The records have been successfully inserted!')

        # Verificar cantidad de registros
        cursor.execute(f'SELECT COUNT(*) FROM {name_table}')
        print(f'Total number of records uploaded: {cursor.fetchone()[0]}')

end_time = time.time()
print(f'TOTAL TIME: {round(((end_time - start_time) / 60), 2)} MIN.')
