// Twitter Hash-tag Analysis - Count the trending hash-tags and arrange them in the descending order
//data format can be found in spark-twitter-exformat.txt

package twitter_hash_analysis

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.flume._
import scala.util.parsing.json._

object HashtagAnalyzer {
	def main(args: Array[String]) {

		//check args.length
		if (args.length < 2) {
			System.err.println("Usage: Hashtag Analyzer <hostname> <port>")
			System.exit(1)
		}

		//set spark app name
		val sparkConf = new SparkConf().setAppName("Twitter hashtag parser")
		//initialize new spark streaming context
		val ssc = new StreamingContext(sparkConf, Seconds(1))
		//initialize stream
		val rawLines = FlumeUtils.createStream(ssc, args(0), args(1).toInt)

		//read in data from stream
		val result = rawLines.map{record => {
			(new String(record.event.getBody().array()))
			}
		}

		//parse json, then extracts tweet text as string; example below
		// "Portfolio Risk Analytics #manager Lending Club, San Francisco, CA. http://t.co/VRxIuCqF3w"
		.map{ line => {
			val parsedJson = JSON.parseFull(line)
			parsedJson.get.asInstanceOf[Map[String, Any]]("text").asInstanceOf[String]
			}
		}

		//parses tweet into each space-separated word
		.flatMap { tweet => {
			tweet.split(" ")
			}
		}

		//attaches a value of 1 to each word
		.map{ rec => (rec, 1)}

		//filter for only words that begin with a "#" symbol
		.filter { token => token._1.startsWith("#") }

		//reduce the rdd by summing instances of each hastag within the window
		.reduceByKeyAndWindow((a:Int, b:Int) => a+b, Seconds(120), Seconds(30))

		//sort the rdd by instances of each hashtag in descending order
      	.transform{rec =>{
         	rec.sortBy(_._2, false)
      		}
	    }


		result.print()
		ssc.start()
		ssc.awaitTermination()
	}
}
