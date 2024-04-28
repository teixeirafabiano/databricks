# Databricks notebook source
#display(dbutils.fs.ls("dbfs:/databricks-datasets/COVID/covid-19-data/live/us-counties.csv"))
display(dbutils.fs.ls("dbfs:/databricks-datasets/iot-stream/data-device/"))

# COMMAND ----------

spark.read.format('json').load('dbfs:/databricks-datasets/iot-stream/data-device/part-00000.json.gz').display()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

#inputPath = "/databricks-datasets/structured-streaming/events/"
inputPath = "/databricks-datasets/iot-stream/data-device/"

# Definindo o Schema do DataSource.
#jsonSchema = StructType([ StructField("time", TimestampType(), True), StructField("action", StringType(), True) ])
jsonSchema = StructType([ StructField("calories_burnt", LongType(), True), 
                         StructField("device_id", IntegerType(), True), 
                         StructField("num_steps", IntegerType(), True), 
                         StructField("timestamp", TimestampType(), True) 
                        ])

streamingInputDF = (
  spark
    .readStream
    .schema(jsonSchema)               
    .option("maxFilesPerTrigger", 1)  # Emularemos o aspecto de Streaming realizando a leitura de um dataset por vez.
    .json(inputPath)
)

# Realizaremos um agrupamento dos eventos (Open e Close) por hora.
streamingCountsDF = (
  streamingInputDF
    .groupBy(
      streamingInputDF.device_id,
      window(streamingInputDF.timestamp, "1 hour"))
    .sum()
)

# COMMAND ----------

# Aqui criaremos uma query interativa, conforme a ingestão de um novo dataset ocorre, essa query é "atualizada".
query = (
  streamingCountsDF
    .writeStream
    .format("memory")        # Armazenando as informações em memória.
    .queryName("contagem")   # Nome da tabela criada em memória.
    .outputMode("complete")  # Modo Complete de Output.
    .start()
)

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC /* Consultando os indicadores utilizando SQL, repare que ao longo do tempo, executando diversas vezes esta query faz com que a tabela apresente resultados diferentes ao longo do processo de ingestão. */
# MAGIC select device_id, date_format(window.end, "MMM-dd HH:mm") as timestamp, count from contagem order by timestamp, device_id

# COMMAND ----------


