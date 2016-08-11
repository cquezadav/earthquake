import scala.reflect.runtime.universe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext

import com.datastax.spark.connector.toRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

class ProcessCountByHour(sqlContext: SQLContext) extends Serializable{
  def process() {

    val batchDF: DataFrame = sqlContext
      .read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "earthquakes_analisys", "table" -> "earthquakes_raw_data"))
      .load() //.where("state = 'Alaska'")

    batchDF.registerTempTable("raw_data")

    val batchQuery = sqlContext.sql("SELECT state, city, date, hour, count(*) as count FROM raw_data GROUP BY state, city, date, hour")
    //batchQuery.foreach(println)

    val mapBatchQuery: RDD[EarthquakeCountByHour] = batchQuery.map { x: Row =>
      val state = x.getAs[String]("state")
      val city = x.getAs[String]("city")
      val date = x.getAs[String]("date")
      val hour = x.getAs[String]("hour")
      val count = x.getAs[Long]("count").toInt
      EarthquakeCountByHour(state, city, date, hour, count)
    }

    mapBatchQuery.saveToCassandra("earthquakes_analisys", "earthquakes_count_by_hour")

  }

  case class EarthquakeCountByHour(state: String, city: String, date: String, hour: String, count: Integer)

}
