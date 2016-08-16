import scalaj.http.Http
import spray.json.JsValue
import spray.json.pimpString

class APIDataConsumer() {
  def consume(startDateTime: String, endDateTime: String): (Integer, String) = {

    val URL = "http://earthquake.usgs.gov/fdsnws/event/1/query.geojson"
    val response = Http(URL)
      .timeout(connTimeoutMs = 5000, readTimeoutMs = 20000)
      .param("starttime", startDateTime)
      .param("endtime", endDateTime)
      .asString.body
    //println(response.toString());

    val jsonResponse = response.toString().parseJson
    val measuresCount = getMeasuresCount(jsonResponse)
    //meta.foreach { x => x. }
    val jsonResponseStr = jsonResponse.toString()

    return (measuresCount, jsonResponseStr)

  }

  def getMeasuresCount(json: JsValue): Integer = {
    val metadata = json.asJsObject.getFields("metadata")
    var count = 0
    metadata.foreach { x =>
      val xx = x.asJsObject.getFields("count")
      xx.foreach { x =>
        count = Integer.parseInt(x.toString())
      }
    }
    return count
  }
}
