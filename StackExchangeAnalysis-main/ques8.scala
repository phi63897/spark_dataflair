//8. The distribution of number of questions closed per month
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

import scala.xml.XML

object ques8 {
  def main(args: Array[String]) = {
    System.setProperty("hadoop.home.dir", "C://hadoop-2.6.0")
    System.setProperty("spark.sql.warehouse.dir", "C://Spark_warehouse")

    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    val format2 = new SimpleDateFormat("yyyy-MM");

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
        //parse data to return (closeDate, 1)
        val xml = XML.loadString(line)

        var closeDate = "";
        if (xml.attribute("ClosedDate") != None)
        {
          val clDate = xml.attribute("ClosedDate").get.toString()
          closeDate = format2.format(format.parse(clDate))
        }
        //			  (closeDate, line)
        (closeDate, 1)
      }
      }
      //filters questions that are not closed
      .filter{ data => {data._1.length() > 0}
      }
      //reduces RDD by month
      .reduceByKey(_ + _)

    //resulting RDD is (month, number of questions)
    result.foreach { println }

    spark.stop
  }
}