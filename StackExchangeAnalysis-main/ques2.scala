// 2. Monthly questions count –provide the distribution of number of questions asked per month

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

import scala.xml.XML

object ques2 {
  def main(args: Array[String]) = {
    System.setProperty("hadoop.home.dir", "C://hadoop-2.6.0")
    System.setProperty("spark.sql.warehouse.dir", "C://Spark_warehouse")

    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    val format2 = new SimpleDateFormat("yyyy-MM");

    val spark = SparkSession
      .builder
      .appName("QuesPerMonth")
      .master("local")
      .getOrCreate()
    val data = spark.read.textFile("C://Spark_input/Posts.xml").rdd
    val result =
      data.filter {line => {line.trim().startsWith("<row")}}
        //parse xml to gather each question (PostTypeID = 1)
      .filter { line => {
        line.contains("PostTypeId=\"1\"")
      }}
        //rdd contains creationDate of every question
      .flatMap {line => {
        val xml = XML.loadString(line)
        xml.attribute("CreationDate")
      }}
        //parses creationDate into usable dateFormats
      .map { line => {
        (format2.format(format.parse(line.toString())), 1)
      }}
        //reduces RDD by month
      .reduceByKey(_ + _)

    //resulting RDD contains number of questions for month
    result.foreach { println }

    spark.stop
  }
}
