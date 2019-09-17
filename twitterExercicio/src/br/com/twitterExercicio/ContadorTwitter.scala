package br.com.twitterExercicio

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ContadorTwitter {
  
  def main(args: Array[String]) {
  
     val sc = new SparkContext("local[*]", "ContadorTwitter")
     val linhas = sc.textFile("Sentiment Analysis Dataset.csv")
      
     val header = linhas.first() 
               
     val palavras =  linhas.filter(row => row != header).flatMap(l => l.split(",")(3).split(" "))
     val hashtags = palavras.filter(word => word.startsWith("#"))
     val hashtagKey = hashtags.map(hashtag => (hashtag.toUpperCase(),1))
     val hashtagcontador = hashtagKey.reduceByKey((x, y) => x + y)
     val ordenacao = hashtagcontador.sortBy(x => x._2, false) 
     
     ordenacao.take(10).foreach(println)
     
    
  }
    
}