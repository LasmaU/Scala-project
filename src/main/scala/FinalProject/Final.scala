package FinalProject

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession


object Final {

  def main(args: Array[String]): Unit = {
    println("Starting FINAL project, good luck!")
    val conf = new SparkConf().setAppName("Working with Spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    for (arg <- args){
      println(s"My arg is $arg")
    }

    if (args.length >=2){
      val spark = init(Array("A","B"))
      println("*"* 60)


      Processing.getTwoDFofBooks(args(1), args(0),spark)

    }

    def init(configArguments: Array[String]): SparkSession={
      val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local").getOrCreate()
      spark
    }


  }

}
