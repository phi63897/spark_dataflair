//7. Number of questions which are active for more than 6 months
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

import scala.xml.XML

object ques7 {
  def main(args: Array[String]) = {
    System.setProperty("hadoop.home.dir", "C://hadoop-2.6.0")
    System.setProperty("spark.sql.warehouse.dir", "C://Spark_warehouse")

    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    val spark = SparkSession
      .builder
      .appName("QuesActive")
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
        //parses line to return (creationDate, LastActivityDate, line)
        val xml = XML.loadString(line)
        (xml.attribute("CreationDate").get,  xml.attribute("LastActivityDate").get, line)
        //			  data._1                              data._2                              data._3
      }
      }
      .map{ data => {
        //formats creationDate from parsed text
        val crDate = format.parse(data._1.text)
        //returns time Object
        val crTime = crDate.getTime;

        //formats lastActivityDate from parsed text
        val edDate = format.parse(data._2.text)
        //returns time Object
        val edTime = edDate.getTime;

        //calculate difference between two time Objects
        val timeDiff : Long = edTime - crTime

        //returns line of data with necessary time details
        (crDate, edDate, timeDiff, data._3)
        //			 data._1 data._2 data._3 data._4
      }
      }

      //filters line to return questions where timeDiff is > 6 months
      .filter { data => { data._3 / (1000 * 60 * 60 * 24) > 30*6}
      }

    //resulting RDD is all questions that are active for more than 6 months
    //result.foreach { println }
    println(result.count())

    spark.stop
  }
}