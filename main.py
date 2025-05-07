import boto3
import psycopg2
import csv
from psycopg2 import sql

# Configuración de PostgreSQL
db_config = {
    'host': 'tu_host_postgres',
    'user': 'tu_usuario',
    'password': 'tu_contraseña',
    'database': 'tu_base_de_datos',
    'port': '5432'  # Puerto por defecto de PostgreSQL
}

# Configuración de S3
nombreBucket = "gcr-output-01"
ficheroCSV = "data.csv"
tabla_a_exportar = "tu_tabla"  # Cambia esto por el nombre de tu tabla

def listar_bases_datos_y_tablas():
    try:
        # Conectar a PostgreSQL (sin especificar base de datos para listar todas)
        conexion = psycopg2.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database='postgres'  # Base de datos por defecto
        )
        conexion.autocommit = True
        cursor = conexion.cursor()
        
        # Listar todas las bases de datos
        cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        print("\nBases de datos disponibles:")
        for db in cursor.fetchall():
            print(f"- {db[0]}")
        
        # Listar tablas de la base de datos actual
        if 'database' in db_config:
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE';
            """)
            print(f"\nTablas en la base de datos '{db_config['database']}':")
            for table in cursor.fetchall():
                print(f"- {table[0]}")
        
    except Exception as e:
        print(f"Error al listar bases de datos y tablas: {e}")
    finally:
        if 'conexion' in locals() and not conexion.closed:
            cursor.close()
            conexion.close()

def exportar_tabla_a_csv():
    try:
        # Conectar a PostgreSQL
        conexion = psycopg2.connect(**db_config)
        cursor = conexion.cursor()
        
        # Obtener datos de la tabla
        query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(tabla_a_exportar))
        cursor.execute(query)
        resultados = cursor.fetchall()
        
        if not resultados:
            print("La tabla está vacía")
            return False
        
        # Obtener nombres de columnas
        col_names = [desc[0] for desc in cursor.description]
        
        # Escribir a CSV
        with open(ficheroCSV, 'w', newline='') as archivo_csv:
            writer = csv.writer(archivo_csv)
            writer.writerow(col_names)  # Escribir encabezados
            writer.writerows(resultados)
        
        print(f"Datos exportados correctamente a {ficheroCSV}")
        return True
        
    except Exception as e:
        print(f"Error al exportar datos: {e}")
        return False
    finally:
        if 'conexion' in locals() and not conexion.closed:
            cursor.close()
            conexion.close()

def subir_a_s3():
    try:
        s3 = boto3.client('s3')
        response = s3.upload_file(ficheroCSV, nombreBucket, ficheroCSV)
        print("Archivo subido correctamente a S3")
        return True
    except Exception as e:
        print(f"Error al subir a S3: {e}")
        return False

if __name__ == "__main__":
    # Primero listamos las bases de datos y tablas disponibles
    listar_bases_datos_y_tablas()
    
    # Luego procedemos con la exportación y subida a S3
    if exportar_tabla_a_csv():
        if subir_a_s3():
            print("Ingesta completada exitosamente")
        else:
            print("Error en la subida a S3")
    else:
        print("Error en la exportación de datos")
