package br.com.testerdd

import org.apache.spark.SparkContext

object ClienteVenda {
  
  def main(args: Array[String]) {
    
    val sc = new SparkContext("local[*]", "ClienteVenda")
    
    val cliente = sc.textFile("clientes.csv")
    
    val clikv = cliente.filter(linha => linha.length > 0).map(linha => (linha.split(",")(0), linha.split(",")(1)))

    clikv.take(10).foreach(println)
    
    val vendas = sc.textFile("vendasMaio2016.csv")

    val vendaskv = vendas.filter(linha => linha.length() >0).map(v => (v.split(",")(1), (v.split(",")(0))))
    
    val cliven = clikv.join(vendaskv)
    
    cliven.take(10).foreach(println)
    
    val cidprod = cliven.map(r => r._2)
    
    cidprod.take(10).foreach(println)
    
    val cidgrup = cidprod.groupByKey()
    
    cidgrup.take(10).foreach(println)
    
  }
}