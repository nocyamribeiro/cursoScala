package br.com.aulaSparkSQL

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object AulaSQL {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[*]", "GastoMinisterio")

    val spark = SparkSession
      .builder()
      .appName("GastoMinisterio")
      .getOrCreate()

    //    val cliDF = spark.read.csv("clientes.csv")
    //    cliDF.show();

    //cliDF.select($"c_0")

    val esquema = StructType(
      StructField("idcli", StringType) ::
        StructField("cidade", StringType) ::
        StructField("sexo", StringType) ::
        StructField("idade", IntegerType) :: Nil)

    val cliDF = spark.read.schema(esquema).csv("clientes.csv")

    cliDF.printSchema

    val cliDFH = spark.read.option("header", "true").csv("clientesComCabecalho.csv")

    cliDFH.printSchema

    //Para reconhecer o $ no filter
    import spark.implicits._

    //Filtra a cidade
    cliDF.filter($"cidade" === "Rio de Janeiro").show()

    //Exibe somente o id do cliente filtrado
    cliDF.filter($"cidade" === "Rio de Janeiro").select($"idcli").show()

    cliDF.filter($"cidade" === "Rio de Janeiro").select("idcli", "sexo").show()
    
    cliDF.filter($"cidade" === "Rio de Janeiro" && $"sexo" === "F").select("idcli", "sexo").show()
    
    val r2 = cliDF.groupBy($"cidade").count
    
    r2.show
    
    
    
  }
}