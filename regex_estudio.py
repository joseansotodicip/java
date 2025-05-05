import numpy as np
import pandas as pd
import json, time, datetime, csv, re, requests
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from functools import reduce
from operator import concat

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import types
from pyspark.conf import SparkConf
from pyspark.sql import SQLContext, HiveContext, SparkSession
import os
from pyspark.sql.utils import AnalysisException
from warnings import warn

from pyspark.errors.exceptions.captured import unwrap_spark_exception
from pyspark.rdd import _load_from_socket
from pyspark.sql.pandas.serializers import ArrowCollectSerializer
from pyspark.sql.pandas.types import _dedup_names
from pyspark.sql.types import ArrayType, MapType, TimestampType, StructType, DataType, _create_row
from pyspark.sql.utils import is_timestamp_ntz_preferred
from pyspark.traceback_utils import SCCallSiteSync
from pyspark.errors import PySparkTypeError
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import lit, max as spark_max
os.environ['SPARK_LOCAL_IP'] = 'localhost'
os.environ['SPARK_LOCAL_HOSTNAME'] = 'localhost'
appName = "Infolab"
master = "local"


from pyspark.sql import functions as F
conf = SparkConf() \
 .setAppName(appName) \
 .setMaster(master) \
 .set("spark.driver.extraClassPath", r"C:\mssql-jdbc-12.6.0.jre8.jar") \
.setAll([
    ('spark.app.name', 'Infolab'),
    ("spark.driver.memory", "4g"),  # Reducido a 4GB para el controlador
    ("spark.executor.memory", "4g"),  # Ajustar la memoria de los ejecutores a 4GB
    ("spark.executor.cores", "2"),  # Reducir los núcleos para que la memoria sea suficiente
    ("spark.executor.instances", "2"),  # Reducir instancias para manejar menos carga
    ("spark.cores.max", "4"),  # Máximo de 4 núcleos para no sobrecargar
    ("spark.network.timeout", "600s"),
    ("spark.executor.heartbeatInterval", "60s"),
    ("spark.sql.shuffle.partitions", "100"),  # Reducir particiones para evitar sobrecarga
    ("spark.executor.memoryOverhead", "512m"),  # Reducir memoria de sobrecarga
    ("es.internal.spark.sql.pushdown.strict", "true"),
("spark.memory.offHeap.enabled", "true"),
("spark.memory.offHeap.size", "2g"),
("spark.sql.autoBroadcastJoinThreshold", "10m"),  # Reduce el umbral de broadcast
("spark.memory.fraction", "0.6")  
])

spark = SparkSession.builder.config(conf=conf).getOrCreate()

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)


database = "HGM_WEB"
user = "sa"
password = "Dicipa12"
servername ="DCPNAT008\SQL_2016"
table='dbo.Dat_Atencion'
url = "jdbc:sqlserver://"+servername+";DatabaseName= " + database + ";encrypt=true;trustServerCertificate=true;applicationName=PySparkApp"



column_Cat_Laboratorio=['LaboratorioNombre']

Cat_Laboratorio= spark.read.format("jdbc") \
    .option("url", url) \
    .option("dbtable", f"(SELECT {', '.join(column_Cat_Laboratorio)} FROM dbo.Cat_Laboratorio) AS tmp") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()


columns_Cat_Prueba = ["PruebaId", "PruebaDescripcion"]


Cat_Prueba= spark.read.format("jdbc") \
 .option("url", url) \
 .option("dbtable", f"(SELECT {', '.join(columns_Cat_Prueba)} FROM dbo.Cat_Prueba) AS tmp") \
 .option("user", user) \
 .option("password", password) \
 .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
 .load()

column_seccion=['SeccionId','SeccionDescripcion','SeccionEstatus']

Cat_Seccion= spark.read.format("jdbc") \
    .option("url", url) \
    .option("dbtable", f"(SELECT {', '.join(column_seccion)} FROM dbo.Cat_Seccion) AS tmp") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

columns_estudio=["EstudioId",'EstudioDescripcion','SeccionId']
Cat_Estudio= spark.read.format("jdbc") \
    .option("url", url) \
    .option("dbtable",  f"(SELECT {', '.join(columns_estudio)} FROM dbo.Cat_Estudio) AS tmp") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

column_Det_EstudioPrueba= ['EstudioId','PruebaId']

Det_EstudioPrueba= spark.read.format("jdbc") \
    .option("url", url) \
    .option("dbtable", f"(SELECT {', '.join(column_Det_EstudioPrueba)} FROM Det_EstudioPrueba) AS tmp") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

database = "regex_infolab"
user = "sa"
password = "Dicipa12"
servername ="25.36.193.49"
table='dbo.estudios_prueba_unico'
url = "jdbc:sqlserver://"+servername+";DatabaseName= " + database + ";encrypt=true;trustServerCertificate=true;applicationName=PySparkApp"

estudios_prueba_unico= spark.read.format("jdbc") \
    .option("url", url) \
    .option("dbtable",table ) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()




Det_EstudioPrueba = Det_EstudioPrueba.join(Cat_Estudio, on=['EstudioId'], how='inner')
Det_EstudioPrueba = Det_EstudioPrueba.join(Cat_Prueba, on=['PruebaId'], how='inner')

df = Det_EstudioPrueba.crossJoin(Cat_Laboratorio)



df1=df.select('PruebaDescripcion','EstudioDescripcion','PruebaId','EstudioId','LaboratorioNombre').distinct()

from pyspark.sql.functions import upper, trim
from functools import reduce
estudios_prueba_unico1=estudios_prueba_unico.select('PruebaDescripcion','EstudioDescripcion')
def escape_regex(text):
    return text.replace(" ", r"\s*")

# Crea las condiciones dinámicamente
condiciones = [
    (col("PruebaDescripcion").rlike(f"(?i){escape_regex(row['PruebaDescripcion'])}") & 
     col("EstudioDescripcion").rlike(f"(?i){escape_regex(row['EstudioDescripcion'])}"))
    for row in estudios_prueba_unico1.collect()
]

# Ahora usamos reduce correctamente para combinar las condiciones con OR
filtro = reduce(lambda a, b: a | b, condiciones)

# Aplica el filtro al DataFrame original
df_filtrado = df1.filter(filtro)
def insert_to_sql_server(df: DataFrame):
    database = "regex_infolab"

    servername ="25.36.193.49"
    jdbc_url = "jdbc:sqlserver://"+servername+";DatabaseName= " + database + ";encrypt=true;trustServerCertificate=true"
    properties = {
        "user": "sa",
        "password": "Dicipa12",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    df.write.jdbc(jdbc_url, "estudios_prueba_unico", mode="append", properties=properties)


insert_to_sql_server(df_filtrado)