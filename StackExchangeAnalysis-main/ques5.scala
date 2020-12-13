//5. The questions that doesn�t have any answers � Number of questions with �0� number of answers
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

import scala.xml.XML

object ques5
{
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
        //parses line to return [ int(answerCount), line]
        val xml = XML.loadString(line)
        (Integer.parseInt(xml.attribute("AnswerCount").getOrElse(0).toString()), line)
      }
      }
      //filters set by if the answerCount is 0
      .filter{x => { x._1 == 0 }
      }

    //resulting RDD contains questions with 0 answers
    //result.foreach { println }
    println(result.count())

    spark.stop
  }
}
