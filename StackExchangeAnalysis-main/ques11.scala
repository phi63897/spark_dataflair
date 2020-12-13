//11. Number of question with specific tags (nosql, big data) which was asked in the specified time range (from 01-01-2015 to 31-12-2015)
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

import scala.xml.XML

object ques11 {
  def main(args: Array[String]) = {
    System.setProperty("hadoop.home.dir", "C://hadoop-2.6.0")
    System.setProperty("spark.sql.warehouse.dir", "C://Spark_warehouse")

    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    val format2 = new SimpleDateFormat("yyyy-MM");
    val format3 = new SimpleDateFormat("yyyy-MM-dd");

    val startTime = format3.parse("2015-01-01").getTime
    val endTime = format3.parse("2015-01-31").getTime

    val spark = SparkSession
      .builder
      .appName("AvgAnsTime")
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
        //parses data to return (crDate, tags, line)
        val xml = XML.loadString(line)
        val crDate = xml.attribute("CreationDate").get.toString()
        val tags = xml.attribute("Tags").get.toString()
        (crDate, tags, line)
      }
      }
      .filter{ data => {
        //filters data by valid results
        var flag = false
        //converts crDate into valid Time object
        val crTime = format.parse(data._1.toString()).getTime
        //checks to see if question contains query tags and that crTime is in query Time range
        if (crTime > startTime && crTime < endTime && (data._2.toLowerCase().contains("bigdata") ||
          data._2.toLowerCase().contains("hadoop") || data._2.toLowerCase().contains("spark")))
          flag = true
        flag
      }
      }
    //resulting RDD is set of questions containing query tags and in time range
    result.foreach { println }
    println(result.count())

    spark.stop
  }
}
