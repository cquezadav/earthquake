import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

import kafka.serializer.StringDecoder
import scalaj.http.Http
import spray.json.DefaultJsonProtocol
import spray.json.pimpString
import kafka.producer.ProducerConfig
import java.util.Properties
import kafka.producer.Producer
import kafka.producer.KeyedMessage

import com.github.nscala_time.time.Imports._

class KafkaProducer() {
  def produce(jsonMessage: String): Unit = {

    //println(jsonMessage)
    val kafka_broker = "192.168.1.40:9092"
    val kafka_topic = "earthquake2"

    val props: Properties = new Properties()
    props.put("metadata.broker.list", kafka_broker)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("group.id", "eq-1")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    producer.send(new KeyedMessage[String, String](kafka_topic, jsonMessage))

    //    val events = sc.parallelize(jsonstr :: Nil)
    //    val df = sqlContext.read.json(events)
    //    df.select("metadata.count").show()
    //
    //    return df
  }
}
