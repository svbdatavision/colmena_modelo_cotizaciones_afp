import pandas as pd
import logging
from _conn import connection  # Importar la función de conexión desde _conn.py
import csv

# Configurar el logger
logging.basicConfig(filename='logs/errors.log', level=logging.ERROR, format='%(asctime)s %(levelname)s %(module)s %(message)s')
logger = logging.getLogger(__name__)

# Inicializar lista de salida
output = []

# Leer el CSV, transformar los datos y convertir todas las columnas a tipo str
try:
    with open('output/certificados.csv', 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';', escapechar='\\', quotechar='"')
        
        for row in reader:
            out = row
            out['RUT_L11'] = row['rut'].replace('.','').zfill(11) if row['rut'] != '' else ''
            out['es_metadata'] = row['es_metadata'] == 'True'
            out['es_cert_cot'] = row['es_cert_cot'] == 'True'
            out['es_dif'] = row['es_dif'] == 'True'
            print(out)
            output.append(out)


    # Convertir la lista de salida a un DataFrame y cambiar los nombres de las columnas a mayúsculas
    #df = pd.DataFrame(output).astype(str)
    df = pd.DataFrame(output)
    df.columns = map(str.upper, df.columns)
    df["METADATA_CREADATE"]=df["METADATA_CREADATE"].str.replace("'","")
    df["METADATA_MODDATE"]=df["METADATA_MODDATE"].str.replace("'","")
    #df["ES_DIF"]=df["ES_DIF"].map({'':False}).fillna(False)
    #df["ES_CERT_COT"]=df["ES_CERT_COT"].map({'':False}).fillna(False)
    #df["ES_METADATA"]=df["ES_METADATA"].map({'':False}).fillna(False)

    for index, row in df.iterrows():
        print(row)


    # Mostrar los primeros registros del DataFrame
#    print("Datos del CSV:")
#    print(df.head())
    
    # Mostrar las columnas del DataFrame
#    print("Columnas del DataFrame:")
#    print(df.columns)
    
    # Mostrar los tipos de las columnas del DataFrame
#    print("Tipos de las columnas del DataFrame:")
#    print(df.dtypes)


except Exception as e:
    logger.error(f"Error al procesar CSV: {e}")
    raise


# Inicializar ctx como None
ctx = None

# Conectar a Snowflake y ejecutar consultas SQL
try:
    ctx = connection()
    
    # Usar el warehouse específico
    ctx.cursor().execute('USE WAREHOUSE P_OPX')
    
    # Insertar datos en la tabla usando pandas
    for index, row in df.iterrows():
        print(row)
#  print(row["ES_METADATA"])
        insert_query = f"""
        INSERT INTO OPX.P_DDV_OPX.AFP_CERTIFICADOS_OUTPUT
        (DOC_IDN, LINK, PERIODO_PRODUCCION, FECHA_INGRESO, METADATA_CREATOR, METADATA_PRODUCER, METADATA_CREADATE, METADATA_MODDATE, ES_METADATA, AFP, ES_CERT_COT, CODVER, RUT, RUT_L11, RES_AFP, ES_DIF, RES_DIF)
        VALUES
        ('{row['DOC_IDN']}', '{row['LINK']}', '{row['PERIODO_PRODUCCION']}', '{row['FECHA_INGRESO']}', '{row['METADATA_CREATOR']}', '{row['METADATA_PRODUCER']}', '{row['METADATA_CREADATE']}', '{row['METADATA_MODDATE']}', '{row['ES_METADATA']}', '{row['AFP']}', '{row['ES_CERT_COT']}', '{row['CODVER']}', '{row['RUT']}', '{row['RUT_L11']}', '{row['RES_AFP']}', '{row['ES_DIF']}', '{row['RES_DIF']}')
	"""
        ctx.cursor().execute(insert_query)
    
    print("Datos insertados en Snowflake.")
except Exception as err:
    logger.error(f"Error al cargar datos en Snowflake: {err}")
    print(f"Error al cargar datos en Snowflake: {err}")
finally:
    if ctx:
        ctx.close()
        print("Conexión a Snowflake cerrada.")