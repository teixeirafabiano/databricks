# Databricks notebook source
display(dbutils.fs.ls("dbfs:/databricks-datasets/structured-streaming/events/"))

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

inputPath = "/databricks-datasets/structured-streaming/events/"

# Definindo o Schema do DataSource.
jsonSchema = StructType([ StructField("time", TimestampType(), True), StructField("action", StringType(), True) ])

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
      streamingInputDF.action,
      window(streamingInputDF.time, "1 hour"))
    .count()
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
# MAGIC select action, date_format(window.end, "MMM-dd HH:mm") as time, count from contagem order by time, action

# COMMAND ----------


