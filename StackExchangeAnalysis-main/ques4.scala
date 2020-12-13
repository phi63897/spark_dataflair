//4. The trending questions which are scored highly by the user � Top 10 highest Scored questions
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

import scala.xml.XML

object ques4 {
  def main(args: Array[String]) = {
    System.setProperty("hadoop.home.dir", "C://hadoop-2.6.0")
    System.setProperty("spark.sql.warehouse.dir", "C://Spark_warehouse")

    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    val format2 = new SimpleDateFormat("yyyy-MM");

    val spark = SparkSession
      .builder
      .appName("ScoreAnalysis")
      .master("local")
      .getOrCreate()

    //Read some example file to a test RDD
    val data = spark.read.textFile("C://Spark_input/Posts.xml").rdd


    val result = data.filter{line => {line.trim().startsWith("<row")}
    }
      //parse xml to gather each question (PostTypeID = 1)
      .filter { line => {line.contains("PostTypeId=\"1\"")}
      }
      .map {line => {
        //parses line to return [ int(score) , line ]
        val xml = XML.loadString(line)
        (Integer.parseInt(xml.attribute("Score").getOrElse(0).toString()), xml.attribute("Title"))
      }
      }
      //sorts questions by score in descending orders
      .sortByKey(false)

    //takes top 10 of questions based on score
    result.take(10).foreach(println)
    //result.foreach { println }


    spark.stop
  }
}
  