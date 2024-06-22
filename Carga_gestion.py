import pandas as pd
import pyodbc
import glob
import os

def extract_from_xlsx(file_to_process):
    dataframe = pd.read_excel(file_to_process)
    dataframe['origen'] = os.path.basename(file_to_process)
    dataframe['tipo'] = 'XLSX'
    return dataframe

# Ruta base a la carpeta que contiene los archivos XLSX
base_path = 'C:\\Users\\Luis\\Downloads\\JENKINS_PR\\'

# Crear un DataFrame vacío sin columnas específicas
extracted_data = pd.DataFrame()

# Extraer datos de todos los archivos XLSX en la carpeta base_path
for xlsxfile in glob.glob(base_path + "*.xlsx"):
    print("Procesando archivo:", xlsxfile)
    extracted_data = pd.concat([extracted_data, extract_from_xlsx(xlsxfile)], ignore_index=True)

print("\nDatos extraídos de todos los archivos XLSX:")
print(extracted_data)

# Convertir todas las columnas a tipo str
extracted_data = extracted_data.astype(str)

# Establecer la conexión a la base de datos SQL Server
server = 'DESKTOP-HQ82Q9B\\SQLTEST'
database = 'master'
username = 'Luis'
password = 'prueba123'
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

try:
    # Conectar a SQL Server
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    print("\nConexión exitosa a SQL Server")

    # Validar la conexión ejecutando una consulta simple
    cursor.execute("SELECT @@VERSION;")
    row = cursor.fetchone()
    print("\nVersión de SQL Server:", row[0])

    # Guardar los datos en la tabla GESTIONES_JUDICIAL
    if not extracted_data.empty:
        for index, row in extracted_data.iterrows():
            # Utilizar tuplas para pasar los valores en cursor.execute()
            cursor.execute("""INSERT INTO GESTIONES_JUDICIAL (PRESTAMO, ESTUDIO, DEP, ABOGADO, CÓDIGO, CLIENTE, CAPITAL, TOTAL_DEUDA, MONEDA, ESTADO, FECHA_OPERATIVO, TIPO_DE_GESTIÓN, TIPO_DE_CONTACTO, NOMBRE_DEL_CONTACTO,
                                TELEF_DEL_CONTACTO, RESULTADO, FECHA_PDP, MONTO_DE_PDP, MONTO_DE_PDP_MN, COMENTARIOS) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                           (row['Prestamo'], row['Estudio'], row['DEP'], row['Abogado'], row['Código'], row['Cliente'], row['Capital'], row['Total Deuda'], row['Moneda'], row['Estado'],
                            row['Fecha Operativo'], row['Tipo de Gestión'], row['Tipo de Contacto'], row['Nombre del Contacto'], row['Telef. Del Contacto'], row['Resultado'],
                            row['Fecha PDP'], row['Monto de PDP'], row['Monto de PDP MN'], row['Comentarios acerca de la gestión']))
        conn.commit()
        print("\nDatos guardados en la tabla GESTIONES_JUDICIAL.")

except Exception as e:
    print("\nError al conectar a SQL Server:", e)

finally:
    cursor.close()
    conn.close()
    print("\nConexión cerrada.")
