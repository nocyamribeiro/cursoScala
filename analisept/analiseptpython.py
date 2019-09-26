#!/usr/bin/env python
# coding: utf-8

# In[119]:


import findspark
import os

findspark.init(os.environ.get('SPARK_HOME'))

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as functions
from decimal import Decimal
from pyspark.sql import DataFrame

import numpy as np
import pandas as pd
import sys
from argparse import ArgumentParser
import functools


# In[120]:


spark = SparkSession.builder.appName("GastoMinisterio").getOrCreate()


# In[121]:


#def processarArquivo(caminhoArquivo) :
    


# In[122]:


caminhoArquivo = "hdfs://localhost:9000/user/ubuntu/2019_Viagem.csv"


# In[123]:


def processarArquivo(caminhoArquivo) :
    print(caminhoArquivo)
    to_value = lambda v : Decimal(v.replace(",", "."))
    udf_to_value = functions.udf(to_value)
    df = spark.read.format("csv").option("header", True).option("delimiter", ";").option('encoding', 'windows-1252').csv(caminhoArquivo)   
    df.printSchema()
    df2 = df.withColumn("value", udf_to_value(df["Valor passagens"]))
    orgaos = df2.select("Nome do órgão superior").distinct()
    agrupamento = df2.groupBy("Nome do órgão superior").agg(functions.max("value"), functions.sum("value"), functions.count("value"), functions.avg("value"))
    agrupamento.show(orgaos.count(), truncate = False)
    return agrupamento


# In[124]:


processarArquivo(caminhoArquivo)


# In[125]:


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


# In[126]:


dfUnido = lista[0]
i = 0
for data in lista : 
    if(i < (len(lista)-1)):
        dfUnido = dfUnido.union(lista[i + 1])
        i = i + 1
    


# In[138]:


dfUnido.show(100)


# In[168]:


dfUnido.write.mode('overwrite').csv("hdfs://localhost:9000/user/ubuntu/agrupamento.csv")


# In[ ]:




