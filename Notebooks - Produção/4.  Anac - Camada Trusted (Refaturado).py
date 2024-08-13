# Databricks notebook source
df = spark.read.json('dbfs:/mnt/anac/raw/')

# COMMAND ----------

colunas = ['Aerodromo_de_Destino', 'Aerodromo_de_Origem', 'CLS', 'Categoria_da_Aeronave', 'Classificacao_da_Ocorrência', 'Danos_a_Aeronave', 'Data_da_Ocorrencia', 'Descricao_do_Tipo', 'Fase_da_Operacao', 'Historico', 'Hora_da_Ocorrência', 'ICAO', 'Ilesos_Passageiros', 'Ilesos_Tripulantes', 'Latitude','Longitude', 'Matricula', 'Modelo', 'Municipio', 'Nome_do_Fabricante', 'Numero_da_Ficha', 'Numero_da_Ocorrencia', 'Numero_de_Assentos', 'Operacao', 'Operador', 'Operador_Padronizado', 'PMD', 'PSSO', 'Regiao', 'Tipo_ICAO', 'Tipo_de_Aerodromo', 'Tipo_de_Ocorrencia', 'UF']

# percorrer todas as colunas e fazer a mesma coisa para todas as selecionadas na variavel

for ajuste in colunas:
    df = df.fillna('Sem Registro', subset=[ajuste])

# COMMAND ----------

ajuste_int = ['Lesoes_Desconhecidas_Passageiros', 'Lesoes_Desconhecidas_Terceiros', 'Lesoes_Desconhecidas_Tripulantes', 'Lesoes_Fatais_Passageiros', 'Lesoes_Fatais_Terceiros', 'Lesoes_Fatais_Tripulantes', 'Lesoes_Graves_Passageiros', 'Lesoes_Graves_Terceiros', 'Lesoes_Graves_Tripulantes', 'Lesoes_Leves_Passageiros', 'Lesoes_Leves_Terceiros', 'Lesoes_Leves_Tripulantes']

for loop in ajuste_int:
    df = df\
        .withColumn(loop, df[loop].cast('int'))\
        .fillna(0, subset=[loop])

# COMMAND ----------

df.write.mode('overwrite').parquet('dbfs:/mnt/anac/trusted/anac_trusted.parquet')
