/* SampleApp.scala:
   This application simply counts the number of lines that contain "val" from itself
 */
import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import com.datastax.spark.connector.toRDDFunctions

import scalaj.http.Http
import spray.json.DefaultJsonProtocol
import spray.json.pimpString

object SampleApp extends DefaultJsonProtocol {
  def main(args: Array[String]) {

    val txtFile = "src/main/scala/SampleApp.scala"
    //val conf = new SparkConf().setAppName("Sample Application").setMaster("local[2]").set("spark.cassandra.connection.host", "192.168.1.40")
    val conf = new SparkConf().setAppName("Sample Application").setMaster("local[2]").set("spark.cassandra.connection.host", "192.168.99.100")
    val sc = new SparkContext(conf)

    //val url = "http://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime=2016-07-19%2000:00:00&endtime=2016-07-26%2023:59:59&minmagnitude=5.5&orderby=time"
    val url = "http://earthquake.usgs.gov/fdsnws/event/1/query.geojson"
    val response = Http(url).timeout(connTimeoutMs = 5000, readTimeoutMs = 20000).param("starttime", "2015-12-01 00:00:00").param("endtime", "2015-12-31 23:59:59").asString.body
    //println(response.toString());

    val jsonAst = response.toString().parseJson
    val sqlContext = new SQLContext(sc)
    val jsonstr = jsonAst.toString()
    val events = sc.parallelize(jsonstr :: Nil)
    val df = sqlContext.read.json(events)
    //println(jsonAst)
    //df.printSchema()
    //df.show()
    //df.registerTempTable("eq_table")
    df.select("metadata.count").show()

    val propertiesRecords = df.select(org.apache.spark.sql.functions.explode(df("features"))).toDF("features")
    //propertiesRecords.printSchema()
    //propertiesRecords.show()

    val properties = propertiesRecords.select("features.properties.place", "features.geometry.coordinates", "features.properties.time", "features.properties.mag",
      "features.properties.magType", "features.properties.tsunami", "features.properties.type", "features.properties.alert", "features.properties.sig",
      "features.properties.net", "features.id")

    //properties.printSchema()

    //TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    //properties.foreach { x => println(x.getAs("mag"), x.getAs("place"), x.getAs("time")) }

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

    val longlat = properties.map { x =>
      parseLongitudeLatitude(x.getAs[Seq[Double]]("coordinates"))
    }

    def parseLongToBoolean(num: Long): Boolean = {
      num match {
        case 0 => false
        case 1 => true
        case _ => false
      }
    }

    //longlat.foreach(println)

    val mapping: RDD[Earthquake] = properties.map { x: Row =>
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
      Earthquake(year, month, timestamp, date, hour, unixTimestamp, id, state, city, place, longitude, latitude, magnitude,
        magnitudeType, tsunami, earthquakeType, alert, significantLevel, network)
    }

    //mapping.foreach(println)

    mapping.saveToCassandra("earthquakes_analisys", "earthquakes_raw_data")

    //import sqlContext.implicits._
    //val batchData = sc.cassandraTable("earthquakes_analisys", "earthquakes_raw_data").where("year = '2016' and month = '01'")
    //batchData.foreach(println)
    val batchDF: DataFrame = sqlContext
      .read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "earthquakes_analisys", "table" -> "earthquakes_raw_data"))
      .load() //.where("state = 'Alaska'")

    //    val year = batchDF("year")
    //    val month = batchDF("month")
    //    val hour = batchDF("hour")
    //    val state = batchDF("state")
    //    val city = batchDF("city")
    //    val batchDF2 = batchDF.groupBy(state, city, year, month).count()
    //batchDF2.printSchema()
    //    batchDF2.foreach(println)
    //    println(batchDF.count())

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
    
    //mapBatchAvgQuery.saveToCassandra("earthquakes_analisys", "earthquakes_magnitude_avg_by_day")

  }

  case class Earthquake(year: String, month: String, timestamp: Long, date: String, hour: String, unixTimestamp: Long, id: String, state: String, city: String, place: String,
                        longitude: Double, latitude: Double, magnitude: Double, magnitudeType: String, tsunami: Boolean,
                        earthquakeType: String, alert: String, significantLevel: Long, network: String)

  case class EarthquakeCountByHour(state: String, city: String, date: String, hour: String, count: Integer)

  case class EarthquakeMagnitudeAvgByDay(state: String, city: String, date: String, average: java.math.BigDecimal)
  
}


//val str = "23km ESE of Anza, CA"
//
//    println(cityPattern findFirstIn str)
//    val allMatches = cityPattern.findAllMatchIn(str)
//    allMatches.foreach { x =>
//      println(x.group(1), x.group(2))
//    }

//        implicit class Regex(sc: StringContext) {
//      def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
//    }

//    val california1 = """(?i).*,\s*ca""".r
//    str match {
//      case california1(_) => println("California")
//      case r"""(?i).*,\s*california\s*""" => println("California")
//      case r"""(?i).*,\s*chile\s*""" => println("Chile")
//      case _ =>  println("Unknown")
//    }


