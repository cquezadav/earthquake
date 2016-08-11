import org.apache.spark.SparkConf

import spray.json.DefaultJsonProtocol
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object MainApp extends DefaultJsonProtocol {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Sample Application").setMaster("local[2]").set("spark.cassandra.connection.host", "192.168.1.40")
    //val conf = new SparkConf().setAppName("Sample Application").setMaster("local[2]").set("spark.cassandra.connection.host", "192.168.99.100")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val consumer = new Consumer(sc, sqlContext)
    val rawDataDF = consumer.consume("2015-12-01 00:00:00", "2015-12-02 23:59:59")
    
    val processRawData = new ProcessRawData()
    processRawData.process(rawDataDF)
    
    val countByHour =  new ProcessCountByHour(sqlContext)
    countByHour.process()
    
    val processAvgMagnitudeByDay =  new ProcessAvgMagnitudeByDay(sqlContext)
    processAvgMagnitudeByDay.process()
    
  }
}
