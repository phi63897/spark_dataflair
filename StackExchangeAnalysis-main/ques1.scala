//1. Count the total number of questions in the available data-set and collect the questions id of all the questions
import org.apache.spark.sql.SparkSession

object ques1 {
  def main(args: Array[String]) = {
    System.setProperty("hadoop.home.dir", "C://hadoop-2.6.0")
    System.setProperty("spark.sql.warehouse.dir", "C://Spark_warehouse")

    val spark = SparkSession
      .builder
      .appName("QuesCount")
      .master("local")
      .getOrCreate()

    //Read some example file to a test RDD
    val data = spark.read.textFile("C://Spark_input/Posts.xml").rdd


    val result = data.filter{line => {line.trim().startsWith("<row")}
    }
      //parse xml to gather each question (PostTypeID = 1)
      .filter { line => {line.contains("PostTypeId=\"1\"")}
      }

    //result is rdd of every question type post in xml
    result.foreach { println }
    println("Total Count: " + result.count())

    spark.stop
  }
}

