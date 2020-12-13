//9. The most scored questions with specific tags � Questions having tag hadoop, spark in descending order of score
import org.apache.spark.sql.SparkSession

import scala.xml.XML

object ques9 {
  def main(args: Array[String]) = {
    System.setProperty("hadoop.home.dir", "C://hadoop-2.6.0")
    System.setProperty("spark.sql.warehouse.dir", "C://Spark_warehouse")

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
        //parse line to return (Tags, Score, line)
        val xml = XML.loadString(line)
        (xml.attribute("Tags").get.toString(), Integer.parseInt(xml.attribute("Score").get.toString()), line)
      }
      }
      //filters question by posts with tag hadoop or spark
      .filter { tag => {tag._1.contains("hadoop") || tag._1.contains("spark")}
      }
      //parses data to return (Score, line)
      .map { data => {
        (data._2, data._3)
      }
      }
      //sorts questions by descending score
      .sortByKey(false)

    //resulting RDD is descending set(score) of questions with tag 'hadoop' or 'spark'
    result.foreach { println }
    //			println(result.count())

    spark.stop
  }
}