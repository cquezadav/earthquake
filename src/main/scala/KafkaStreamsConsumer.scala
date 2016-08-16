import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

import kafka.serializer.StringDecoder

class KafkaStreamsConsumer(sparkContext: SparkContext, sqlContext: SQLContext) extends Serializable {
  def consume(): Unit = {
    val kafka_broker = "192.168.1.40:2181"
    val kafka_topic = "earthquake2"

    val ssc: StreamingContext = new StreamingContext(sparkContext, Seconds(1))
    //ssc.checkpoint("checkpoint")

    // http://kafka.apache.org/08/configuration.html -> See section 3.2 Consumer Configs
    val kafkaParams = Map(
      "zookeeper.connect" -> kafka_broker,
      "zookeeper.connection.timeout.ms" -> "20000",
      "group.id" -> "eq-1")

    // Map of (topic_name -> numPartitions) to consume. Each partition is consumed in its own thread
    val topics = Map(
      kafka_topic -> 1)

    // Assuming very small data volumes for example app - tune as necessary.
    val storageLevel = StorageLevel.MEMORY_ONLY
    val processRawData = new ProcessRawData()
    val measuresCounter = new AtomicInteger
    val measuresStreams = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics, storageLevel).map(_._2)
    //measuresStreams.print()
    measuresStreams.foreachRDD { rdd =>
      if (rdd.toLocalIterator.nonEmpty) {
        val measuresDF = sqlContext.read.json(rdd)
        val measuresCount = measuresDF.select("metadata.count")
        measuresCount.collect.foreach { x =>
          val counter = x.getAs[Long]("count").toInt
          println("Stream counter = " + counter)
          measuresCounter.addAndGet(counter)
          println("Measures procesed so far = " + measuresCounter.get.toString())
       }
        processRawData.process(measuresDF)
      }

      //println("Measures procesed so far = " + measuresCounter.get.toString())
    }

    // Listen for SIGTERM and shutdown gracefully.
    sys.ShutdownHookThread {
      //log.info("Gracefully stopping Spark Streaming Application")
      //ssc.stop(stopSparkContext = true, stopGracefully = true)
      ssc.stop(true, true)
      // log.info("Application stopped")
    }

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate (manually or due to any error)
  }

}
