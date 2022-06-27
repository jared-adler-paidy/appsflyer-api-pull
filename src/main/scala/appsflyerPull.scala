import org.apache.spark.sql.SparkSession
import sttp.client3._
import sttp.model._
import sttp.model.Uri
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, SparkSession}

object appsflyerPull {

  def main(args: Array[String]) {


    def createSparkSession(): SparkSession = {
      val builder = SparkSession.builder
      builder.appName("ymir")
      builder.getOrCreate()

    }


    val spark: SparkSession = createSparkSession()
    spark.sparkContext.setLogLevel("WARN")

    import spark.sqlContext.implicits._

    val backend = HttpClientSyncBackend()

    val appID = "com.paidy.paidy"
    val reportName = "installs_report"


    val appsflyerSource = "https://hq.appsflyer.com/export/%s/%s/v5".format(appID, reportName)

    val params =
      Map(
        "api_token" -> "hello",
        "from" -> "2022-06-01 00:00:00",
        "to" -> "2022-06-02 00:00:00",
        "maximum_rows" -> "1000000"
      )

    val endpointWithParams: Uri = uri"$appsflyerSource?$params"

    println(endpointWithParams)

    val response: Identity[Response[Either[String, String]]] =
      basicRequest
      .followRedirects(true)
      .get(endpointWithParams)
      .response(asString)
      .send(backend)

    val responseBody = response.body

    val responseString = responseBody match{
      case Right(responseBody) => responseBody
      case Left(_) => "error"
    }

    val responseLines = responseString.lines.toArray

    val reportCols = responseLines.head.toString.split(",").slice(0, 24)

    val appsflyerSchema =
      StructType(reportCols.map(x => StructField(x.replace(" ", "").toLowerCase,StringType, nullable = true)))

    val csvBody = responseLines.tail.map(x => x.toString) //.replace("%20", " ")
    val csvRDD = spark.sparkContext.parallelize(csvBody).map(x => x.split(",").slice(0, 24))

    val frame = spark.sqlContext.createDataFrame(csvRDD.map(x => Row.fromSeq(x)), appsflyerSchema)

    frame.write.parquet("/Users/jaredadler/downloads/sampleappsflyer.parquet")

  }
}
