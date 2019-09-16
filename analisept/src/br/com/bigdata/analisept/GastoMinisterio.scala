package br.com.bigdata.analisept

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions

object GastoMinisterio {
  
   /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
//    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "GastoMinisterio")   
    
    val spark = SparkSession
    .builder()
    .appName("GastoMinisterio")
    .getOrCreate()
    
    val df = spark.read.format("csv").option("header", "true").option("delimiter", ";").option("encoding", "windows-1252").load("hdfs://localhost:9000/user/maycon/2019_Viagem.csv")
    val to_value = (v:String) => v.replace(",", ".").toDouble
    val udf_to_value = functions.udf(to_value)
    
    val df2 = df.withColumn("value", udf_to_value(df("Valor passagens")))

    val orgaos = df2.select("Nome do órgão superior").distinct()
    
    df2.groupBy("Nome do órgão superior").agg(functions.max("value"), functions.sum("value"), functions.count("value"), functions.avg("value")).show(orgaos.count().intValue(), truncate = false)
    
    import spark.implicits._
    val tuplaOrgaosValor = df2.map(x => (x.getAs[String]("Nome do órgão superior"), x.getAs[Double]("value")))
    
   
    
    
  }
  
}