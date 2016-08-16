import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.Duration

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import com.github.nscala_time.time.Imports.richDateTime
import com.github.nscala_time.time.Imports.richInt

import akka.actor.ActorSystem

import org.apache.log4j.Logger

object MainApp {
  def main(args: Array[String]) {
    val logger = Logger.getLogger(getClass)

    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkConf = new SparkConf()
      .setAppName("Sample Application")
      .setMaster("local[4]")
      //.setMaster("spark://192.168.1.61:7077").set("spark.ui.port","7077")
      .set("spark.cassandra.connection.host", "192.168.1.40")
    //.set("spark.cassandra.connection.host", "192.168.99.100")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val APIconsumer = new APIDataConsumer()
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

    ConsumerScheduler.schedule(APIconsumer)
    val kafkaConsumer = new KafkaStreamsConsumer(sparkContext, sqlContext)
    kafkaConsumer.consume()

  }

  object ConsumerScheduler {
    def schedule(APIconsumer: APIDataConsumer) {
      val logger = Logger.getLogger(getClass)

      val actorSystem = ActorSystem()
      val scheduler = actorSystem.scheduler
      var startDateTime = new DateTime(2010, 4, 1, 0, 0, 0, 0, DateTimeZone.UTC)
      var endDateTime = startDateTime + 1.day - 1.second
      println(startDateTime)
      println(endDateTime)
      val measuresCounter = new AtomicInteger

      val task = new Runnable {
        def run() {
          try {
            println("Consuming....")
            val startDateStr = startDateTime.toString("yyyy-MM-dd HH:mm:ss")
            val endDateStr = endDateTime.toString("yyyy-MM-dd HH:mm:ss")
            println(startDateStr)
            println(endDateStr)
            val APIconsumerResponse = APIconsumer.consume(startDateStr, endDateStr)
            measuresCounter.addAndGet(APIconsumerResponse._1)
            println("json counter = " + measuresCounter)
            val kafkaProducer = new KafkaProducer()
            kafkaProducer.produce(APIconsumerResponse._2)
            startDateTime = endDateTime + 1.second
            endDateTime = startDateTime + 1.day - 1.second
          } catch {
            case e: Exception => {
              println("exception caught: " + e)
              logger.error(e)
            }
          }
        }
      }

      implicit val executor = actorSystem.dispatcher

      scheduler.schedule(
        initialDelay = Duration(1, TimeUnit.SECONDS),
        interval = Duration(1, TimeUnit.SECONDS),
        runnable = task)
    }
  }
}
