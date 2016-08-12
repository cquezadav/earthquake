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

class Consumer(sc: SparkContext, sqlContext: SQLContext) extends DefaultJsonProtocol {
  def consume(startDateTime: String, endDateTime: String): DataFrame = {

    val url = "http://earthquake.usgs.gov/fdsnws/event/1/query.geojson"
    //val response = Http(url).timeout(connTimeoutMs = 5000, readTimeoutMs = 20000).param("starttime", "2015-12-01 00:00:00").param("endtime", "2015-12-31 23:59:59").asString.body
    val response = Http(url).timeout(connTimeoutMs = 5000, readTimeoutMs = 20000).param("starttime", startDateTime).param("endtime", endDateTime).asString.body
    //println(response.toString());

    val jsonAst = response.toString().parseJson
    val jsonstr = jsonAst.toString()

//    val kafka_broker = "192.168.1.40:9092"
//    val kafka_topic = "earthquake"
//
//    val props: Properties = new Properties()
//    props.put("metadata.broker.list", "192.168.1.40:9092")
//    props.put("serializer.class", "kafka.serializer.StringEncoder")
//
//    val config = new ProducerConfig(props)
//    val producer = new Producer[String, String](config)
//    
//    producer.send(new KeyedMessage[String, String](kafka_topic, jsonstr))
    

    val events = sc.parallelize(jsonstr :: Nil)
    val df = sqlContext.read.json(events)
    df.select("metadata.count").show()

    return df
  }
}
