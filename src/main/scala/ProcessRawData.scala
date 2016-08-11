import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import com.datastax.spark.connector.toRDDFunctions

import spray.json.DefaultJsonProtocol

class ProcessRawData() extends Serializable {
  def process(measuresDF: DataFrame) {

    val featuresTable = "features"

    val rawMeasuresDF = measuresDF
      .select(org.apache.spark.sql.functions.explode(measuresDF(featuresTable)))
      .toDF(featuresTable)
      .select("features.properties.place", "features.geometry.coordinates", "features.properties.time", "features.properties.mag",
        "features.properties.magType", "features.properties.tsunami", "features.properties.type", "features.properties.alert",
        "features.properties.sig", "features.properties.net", "features.id")

    val parseStateAndCityPattern = """^.* of (.*)\s*,\s*(.*)\s*$""".r

    def parseStateAndCity(str: String): (String, String) = {
      val allMatches = parseStateAndCityPattern.findAllMatchIn(str)
      var stateCity: (String, String) = ("Unknown", "Unknown")
      allMatches.foreach { x =>
        stateCity = (x.group(2), x.group(1))
      }
      return stateCity
    }

    def parseLongitudeLatitude(coord: Seq[Double]): (Double, Double) = {
      (coord(0), coord(1))
    }

    def parseLongToBoolean(num: Long): Boolean = {
      num match {
        case 0 => false
        case 1 => true
        case _ => false
      }
    }

    val mapRawMeasures: RDD[EarthquakeRawData] = rawMeasuresDF.map { x: Row =>
      val year = new DateTime(x.getAs[Long]("time"), DateTimeZone.UTC).toDateTime.toString("yyyy")
      val month = new DateTime(x.getAs[Long]("time"), DateTimeZone.UTC).toDateTime.toString("MM")
      val timestamp = x.getAs[Long]("time")
      val date = new DateTime(x.getAs[Long]("time"), DateTimeZone.UTC).toDateTime.toString("yyyy/MM/dd")
      val hour = new DateTime(x.getAs[Long]("time"), DateTimeZone.UTC).toDateTime.toString("HH")
      val unixTimestamp = x.getAs[Long]("time")
      val id = x.getAs[String]("id")
      val state = parseStateAndCity(x.getAs("place"))._1
      val city = parseStateAndCity(x.getAs("place"))._2
      val place = x.getAs[String]("place")
      val longitude = parseLongitudeLatitude(x.getAs[List[Double]]("coordinates"))._1
      val latitude = parseLongitudeLatitude(x.getAs[List[Double]]("coordinates"))._2
      val magnitude = x.getAs[Double]("mag")
      val magnitudeType = x.getAs[String]("magType")
      val tsunami = parseLongToBoolean(x.getAs("tsunami"))
      val earthquakeType = x.getAs[String]("type")
      val alert = x.getAs[String]("alert")
      val significantLevel = x.getAs[Long]("sig")
      val network = x.getAs[String]("net")

      EarthquakeRawData(year, month, timestamp, date, hour, unixTimestamp, id, state, city, place, longitude, latitude, magnitude,
        magnitudeType, tsunami, earthquakeType, alert, significantLevel, network)
    }

    mapRawMeasures.saveToCassandra("earthquakes_analisys", "earthquakes_raw_data")

  }

  case class EarthquakeRawData(year: String, month: String, timestamp: Long, date: String, hour: String, unixTimestamp: Long, id: String,
                               state: String, city: String, place: String, longitude: Double, latitude: Double, magnitude: Double,
                               magnitudeType: String, tsunami: Boolean, earthquakeType: String, alert: String, significantLevel: Long, network: String)

}
