
//3. Provide the number of posts which are questions and contains specified words in their title (like data, science, nosql, hadoop, spark)

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

import scala.xml.XML

object ques3 {
  def main(args: Array[String]) = {
    System.setProperty("hadoop.home.dir", "C://hadoop-2.6.0")
    System.setProperty("spark.sql.warehouse.dir", "C://Spark_warehouse")

    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    val format2 = new SimpleDateFormat("yyyy-MM");

    val spark = SparkSession
      .builder
      .appName("TitleAnalysis")
      .master("local")
      .getOrCreate()

    val data = spark.read.textFile("C://Spark_input/Posts.xml").rdd

    val result = data.filter{line => {line.trim().startsWith("<row")}
    }
      //parse xml to gather each question (PostTypeID = 1)
      .filter { line => {line.contains("PostTypeId=\"1\"")}
      }
      //gathers the title of every question
      .flatMap {line => {
        val xml = XML.loadString(line)
        xml.attribute("Title")
      }
      }
      //parses questions for titles that contain string (ex. "hadoop:)
      .filter { line => {
        line.mkString.toLowerCase().contains("hadoop")
      }
      }

    //resulting RDD is all questions that contain query string
    //result.foreach { println }
    println ("Result Count: " + result.count())

    spark.stop
  }
}