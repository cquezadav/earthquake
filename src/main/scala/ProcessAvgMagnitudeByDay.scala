import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame

import com.datastax.spark.connector.toRDDFunctions

class ProcessAvgMagnitudeByDay(sqlContext: SQLContext) extends Serializable{
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
      val average = x.getAs[java.math.BigDecimal]("average")
      EarthquakeMagnitudeAvgByDay(state, city, date, average)
    }

    mapBatchAvgQuery.foreach(println)

    mapBatchAvgQuery.saveToCassandra("earthquakes_analisys", "earthquakes_magnitude_avg_by_day")

  }

  case class EarthquakeMagnitudeAvgByDay(state: String, city: String, date: String, average: java.math.BigDecimal)

}




