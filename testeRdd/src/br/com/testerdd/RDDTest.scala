package br.com.testerdd

import org.apache.spark.SparkContext

object RDDTest {
  
  
  def main(args: Array[String]) {
      
    val sc = new SparkContext("local[*]", "RDDTest")
    val logrdd = sc.textFile("access_log")

    val logKV = logrdd.map(linha => (linha.split(" ")(0), linha))

    val ip_1 = logKV.map(par => (par._1, 1))

    val ip_cont = ip_1.reduceByKey((v1, v2) => v1 + v2)

    ip_cont.take(10).foreach(println)
    
    
    
  }
  
}