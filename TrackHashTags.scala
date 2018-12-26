import org.apache.log4j.{Level, Logger}
import java.util.concurrent.atomic._

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}


object TrackHashTags {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    // Set logging level if log4j not configured (override by adding log4j.properties to classpath)
    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }



    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("TwitterPopularTags")


    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None).filter(l=>l.getLang=="en")
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))


    val topCounts = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(120))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))


    topCounts.foreachRDD(rdd => {
      val topList = rdd.take(5)
      //println("\nPopular topics in last 120 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s, count: %s".format(tag, count))}

    })


    ssc.start()
    ssc.awaitTermination()
  }
}
