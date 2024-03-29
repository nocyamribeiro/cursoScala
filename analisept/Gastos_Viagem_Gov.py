#!/usr/bin/env python
# coding: utf-8

# In[35]:


import findspark
import os
import sys

findspark.init(os.environ.get('SPARK_HOME'))

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as functions
from decimal import Decimal
from pyspark.sql import functions as F 


# In[36]:


spark = SparkSession.builder.appName("GastoMinisterio").getOrCreate()


# In[37]:


#converte decimal
to_value = lambda v : Decimal(v.replace(",", "."))


# In[38]:


udf_to_value = functions.udf(to_value)


# In[39]:


caminhoArquivo = '/home/maycon/Cursos/spark/2019_Viagem.csv'


# In[46]:


def processarArquivo(caminhoArquivo) :
    df = spark.read.format("csv").option("header", True).option("delimiter", ";").option('encoding', 'windows-1252').csv(caminhoArquivo)
    df2 = df.withColumn("Max_por_org_sup",    udf_to_value(df["Valor passagens"]))
    df2 = df2.withColumn("Media_por_org_sup", udf_to_value(df2["Valor passagens"]))
    df2 = df2.withColumn("Min_por_org_sup",   udf_to_value(df2["Valor passagens"]))
    df2 = df2.withColumn("Total_por_org_sup", udf_to_value(df2["Valor passagens"]))

    df2 = df2.withColumn("Max_por_destinos", udf_to_value(df2["Valor passagens"]))
    df2 = df2.withColumn("Media_por_destinos", udf_to_value(df2["Valor passagens"]))
    df2 = df2.withColumn("Min_por_destinos", udf_to_value(df2["Valor passagens"]))
    df2 = df2.withColumn("Total_por_destinos", udf_to_value(df2["Valor passagens"]))

    df2 = df2.withColumn("Max_por_cargos", udf_to_value(df2["Valor passagens"]))
    df2 = df2.withColumn("Media_por_cargos", udf_to_value(df2["Valor passagens"]))
    df2 = df2.withColumn("Min_por_cargos", udf_to_value(df2["Valor passagens"]))
    df2 = df2.withColumn("Total_por_cargos", udf_to_value(df2["Valor passagens"]))
    
    df2 = df2.withColumn("Max_por_solicitante", udf_to_value(df2["Valor passagens"]))
    df2 = df2.withColumn("Media_por_solicitante", udf_to_value(df2["Valor passagens"]))
    df2 = df2.withColumn("Min_por_solicitante", udf_to_value(df2["Valor passagens"]))
    df2 = df2.withColumn("Total_por_solicitante", udf_to_value(df2["Valor passagens"]))

    df2 = df2.withColumn("Max_por_org_sup_diaria",    udf_to_value(df2["Valor diárias"]))
    df2 = df2.withColumn("Media_por_org_sup_diaria", udf_to_value(df2["Valor diárias"]))
    df2 = df2.withColumn("Min_por_org_sup_diaria",   udf_to_value(df2["Valor diárias"]))
    df2 = df2.withColumn("Total_por_org_sup_diaria", udf_to_value(df2["Valor diárias"]))
    
    return df2


# In[47]:


argumentos = []
for i in sys.argv :
    if(i.endswith(".csv")): 
        argumentos.append(i);
lista = []
if len(argumentos) == 0 : 
    lista.append(processarArquivo(caminhoArquivo))
else :
    for i in argumentos :
        lista.append(processarArquivo(i)) 


# In[48]:


dfUnido = lista[0]
i = 0
for data in lista : 
    if(i < (len(lista)-1)):
        dfUnido = dfUnido.union(lista[i + 1])
        i = i + 1


# In[50]:


tabela_aggnm_sup = dfUnido.groupBy("Nome do órgão superior").agg(F.max("Max_por_org_sup"), 
                                          F.avg("Media_por_org_sup"), 
                                          F.min("Min_por_org_sup"), 
                                          F.sum("Total_por_org_sup")).sort('Nome do órgão superior')
tabela_aggnm_destino =dfUnido.groupBy("Destinos").agg(F.max("Max_por_destinos"), 
                                          F.avg("Media_por_destinos"), 
                                          F.min("Min_por_destinos"), 
                                          F.sum("Total_por_destinos")).sort('Destinos')
tabela_aggnm_cargo =dfUnido.groupBy("Cargo").agg(F.max("Max_por_cargos"), 
                                          F.avg("Media_por_cargos"), 
                                          F.min("Min_por_cargos"), 
                                          F.sum("Total_por_cargos")).sort('Cargo')



# In[52]:


dfUnido.printSchema()


# In[57]:


tabela_aggnm_solicitante =dfUnido.groupBy("Nome órgão solicitante").agg(F.max("Max_por_solicitante"), 
                                          F.avg("Media_por_solicitante"), 
                                          F.min("Min_por_solicitante"), 
                                          F.sum("Total_por_solicitante")).sort('Nome órgão solicitante')

tabela_aggnm_diaria =dfUnido.groupBy("Nome do órgão superior").agg(F.max("Max_por_org_sup_diaria"), 
                                          F.avg("Media_por_org_sup_diaria"), 
                                          F.min("Min_por_org_sup_diaria"), 
                                          F.sum("Total_por_org_sup_diaria")).sort('Nome do órgão superior')


# In[ ]:


tabela_aggnm_sup.coalesce(1).write.mode('overwrite').option('header', "true").csv("agg_por_org_sup")
tabela_aggnm_destino.coalesce(1).write.mode('overwrite').option('header', "true").csv("agg_por_destinos")
tabela_aggnm_cargo.coalesce(1).write.mode('overwrite').option('header', "true").csv("agg_por_cargo")
tabela_aggnm_solicitante.coalesce(1).write.mode('overwrite').option('header', "true").csv("agg_por_solicitante")
tabela_aggnm_diaria.coalesce(1).write.mode('overwrite').option('header', "true").csv("agg_por_diaria")


# In[ ]:




