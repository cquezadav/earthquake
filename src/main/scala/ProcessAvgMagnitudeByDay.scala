import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame

import com.datastax.spark.connector.toRDDFunctions
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger

object MainApp2 {
  def main(args: Array[String]) {
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
    
    val process = new ProcessAvgMagnitudeByDay(sqlContext)
    process.process()
  }
}

class ProcessAvgMagnitudeByDay(sqlContext: SQLContext) extends Serializable {
  def process() {

    sqlContext.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "earthquakes_analisys", "table" -> "earthquakes_raw_data"))
      .load().registerTempTable("raw_data")

    val batchAvgQuery = sqlContext.sql("SELECT state, city, date, AVG(magnitude) as average FROM raw_data GROUP BY state, city, date")
    batchAvgQuery.printSchema()

    val mapBatchAvgQuery: RDD[EarthquakeMagnitudeAvgByDay] = batchAvgQuery.map { x: Row =>
      val state = x.getAs[String]("state")
      val city = x.getAs[String]("city")
      val date = x.getAs[String]("date")
      val average = x.getAs[Double]("average")
      EarthquakeMagnitudeAvgByDay(state, city, date, average)
    }

    //mapBatchAvgQuery.foreach(println)

    mapBatchAvgQuery.saveToCassandra("earthquakes_analisys", "earthquakes_magnitude_avg_by_day")

  }

  case class EarthquakeMagnitudeAvgByDay(state: String, city: String, date: String, average: Double)

}




