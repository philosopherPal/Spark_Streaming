import org.apache.log4j.{Level, Logger}
import java.util.concurrent.atomic._

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}


object AveTweetLength {
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
    val stream = TwitterUtils.createStream(ssc, None, filters)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val tweets = stream.map(status => status.getText)
    val lengthOfTweets = tweets

      .map(_.length)

    // As we could have multiple processes adding into these running totals at the same time, we'll just Java's AtomicLong class to make sure //
    // that the counters are thread-safe.
    val totalNoOfTweets = new AtomicLong(0)
    val totalNoOfChars = new AtomicLong(0)

    lengthOfTweets.foreachRDD((rdd, _) => {

      val count = rdd.count()
      if (count > 0) {
        totalNoOfTweets.getAndAdd(count)

        totalNoOfChars.getAndAdd(rdd.reduce(_ + _))

        //println(f"| ${totalTweets.get()}%12f | ${totalChars.get()}%16f | ${(totalChars.get() / totalTweets.get())}%7d |")
        println(f"Total Tweets: ${totalNoOfTweets.get()}, Average Length: ${(totalNoOfChars.get() / totalNoOfTweets.get())}")
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

