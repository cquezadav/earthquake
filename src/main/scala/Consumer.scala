import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scalaj.http.Http
import spray.json.DefaultJsonProtocol
import spray.json.pimpString
import org.apache.spark.sql.DataFrame

class Consumer(sc: SparkContext, sqlContext: SQLContext) extends DefaultJsonProtocol {
  def consume(startDateTime: String, endDateTime: String): DataFrame = {

    val url = "http://earthquake.usgs.gov/fdsnws/event/1/query.geojson"
    //val response = Http(url).timeout(connTimeoutMs = 5000, readTimeoutMs = 20000).param("starttime", "2015-12-01 00:00:00").param("endtime", "2015-12-31 23:59:59").asString.body
    val response = Http(url).timeout(connTimeoutMs = 5000, readTimeoutMs = 20000).param("starttime", startDateTime).param("endtime", endDateTime).asString.body
    //println(response.toString());

    val jsonAst = response.toString().parseJson
    val jsonstr = jsonAst.toString()
    val events = sc.parallelize(jsonstr :: Nil)
    val df = sqlContext.read.json(events)
    //df.select("metadata.count").show()

    return df
  }
}
