import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
object UniqueUserCount {
  def main(args: Array[String]): Unit = {
    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }
    val sparkConf = new SparkConf().setAppName("FM")


    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    //val sparkConf = new SparkConf().setAppName("FM").setMaster("local[8]")
    //val sc = new SparkContext(sparkConf)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    //val rdd = ssc.socketTextStream("localhost",9999)
    val localhost = args(0)
    val port = args(1).toInt
    val rdd = ssc.socketTextStream(localhost,port)
    // rdd.print()
    val bin = rdd.map(line => line.toInt.toBinaryString.toLong)
    //bin.print()
    var trailZeroes = bin.map(line=> countTrailingZeros(line))
    //trailZeroes.print()
    var power2 = trailZeroes.window(Seconds(120)).map(line => math.pow(2,line))
    power2.foreachRDD(rdd=>{println(rdd.max().toInt)//println(maximum)
    })

    //println(maximum)
    // /maximum.print()

    //var maximum = power2.foreachRDD(rdd=>rdd.max())

    ssc.start()
    ssc.awaitTermination()


  }
  def countTrailingZeros(s:Long): Int ={
    var x:Long  = s
    if (x == 0) {
      return 0
    }
    var counter = 0
    while (x % 10 == 0) {
      counter+=1
      x /= 10
    }
    counter
  }

}

