//10. List of all the tags along with their counts
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

import scala.xml.XML

object ques10 {
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
        //parses line to return (tags)
        val xml = XML.loadString(line)
        xml.attribute("Tags").get.toString()
      }
      }
      .flatMap { data => {
        //parses data to return space_separated tags
        data.replaceAll("&lt;", " ").replaceAll("&gt;", " ").split(" ")
      }
      }
      //filters out empty space tags
      .filter { tag => {tag.length() > 0 }
      }
      //parses data to return (tag,1)
      .map { data => {
        (data, 1)
      }
      }
      //reduces by tag to gather number of each tag
      .reduceByKey(_ + _)
      //sorts by ascending tag (alphabetically a-z)
      .sortByKey(true)

    //resulting RDD is ascending set(a-z) of (tag, count)
    result.foreach { println }
    //			println(result.count())

    spark.stop
  }
}
