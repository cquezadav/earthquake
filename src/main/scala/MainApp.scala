import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import com.github.nscala_time.time.Imports.richDateTime
import com.github.nscala_time.time.Imports.richInt

import akka.actor.ActorSystem

object MainApp {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Sample Application")
      .setMaster("local[2]")
    //  .set("spark.cassandra.connection.host", "192.168.1.40")
    //val conf = new SparkConf().setAppName("Sample Application").setMaster("local[2]").set("spark.cassandra.connection.host", "192.168.99.100")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val consumer = new Consumer(sc, sqlContext)
    //val rawDataDF = consumer.consume("2015-12-01 00:00:00", "2015-12-02 23:59:59")
    //    
    //    val processRawData = new ProcessRawData()
    //    processRawData.process(rawDataDF)
    //    
    //    val countByHour =  new ProcessCountByHour(sqlContext)
    //    countByHour.process()
    //    
    //    val processAvgMagnitudeByDay =  new ProcessAvgMagnitudeByDay(sqlContext)
    //    processAvgMagnitudeByDay.process()

    var startDateTime = new DateTime(2000, 1, 1, 0, 0, 0, 0, DateTimeZone.UTC)
    var endDateTime = startDateTime + 1.month - 1.second

    println(startDateTime)
    println(endDateTime)

    val actorSystem = ActorSystem()
    val scheduler = actorSystem.scheduler
    val task = new Runnable {
      def run() {
        println("Consuming....")
        val startDateStr = startDateTime.toString("yyyy-MM-dd HH:mm:ss")
        val endDateStr = endDateTime.toString("yyyy-MM-dd HH:mm:ss")
        println(startDateStr)
        println(endDateStr)
        val  consumerDF = consumer.consume(startDateStr, endDateStr)
        //consumerDF.foreach(println)
        startDateTime = endDateTime + 1.second
        endDateTime = startDateTime + 1.month - 1.second
      }
    }

    implicit val executor = actorSystem.dispatcher

    scheduler.schedule(
      initialDelay = Duration(1, TimeUnit.SECONDS),
      interval = Duration(5, TimeUnit.SECONDS),
      runnable = task)

  }

}
