//12. Average time for a post to get a correct answer
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

import scala.xml.XML

object ques12 {
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

    val baseData = data.filter{line => {line.trim().startsWith("<row")}
    }
      //parses posts to return (questionID, AcceptedAnswerId, CreationDate)
      .map {line => {
        val xml = XML.loadString(line)
        //default empty string if no accepted answer
        var aaId = "";
        if (xml.attribute("AcceptedAnswerId") != None)
        {
          aaId = xml.attribute("AcceptedAnswerId").get.toString()
        }
        val crDate = xml.attribute("CreationDate").get.toString()
        val rId = xml.attribute("Id").get.toString()

        (rId, aaId, crDate)
      }
      }
    //returns RDD of (aaID, crDate)
    val aaData = baseData.map{ data => {
      (data._2, data._3)
    }
    }
      //filters by if post has an accepted answer
      .filter{ data => {data._1.length() > 0}}

    //returns RDD of (rID, crDate)
    val rdata = baseData.map{ data => {
      (data._1, data._3)
    }
    }
    //joins previous RDD to return posts with accepted answers
    val joinData = rdata.join(aaData)
      .map{ data => {
        //formats Date object from creation date
        val quesDate = format.parse(data._2._2).getTime
        //formats Date object from answer date
        val ansDate = format.parse(data._2._1).getTime
        //calculates time to get answer
        val diff : Float = ansDate - quesDate
        //converts to hours until answer
        val time : Float = diff/(1000 * 60 * 60)    //millisecond to hour
        time
      }
      }
    //counts for number of posts with answer
    val count = joinData.count()
    //returns average answer time
    val result = joinData.sum() / count

    println(result)
    spark.stop
  }
}
