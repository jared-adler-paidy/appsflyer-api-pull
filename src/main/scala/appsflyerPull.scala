import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.headers.`Set-Cookie`
import akka.http.scaladsl.model._

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Future

object appsflyerPull {

  def main(args: Array[String]) {

    val appID = "id1220373112"
    val reportName = "installs_report"

    implicit val system: ActorSystem = ActorSystem()
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher

    val params =
      Map(
        "api_token" -> "hello",
        "from" -> "2022-06-01 00:00:00",
        "to" -> "2022-06-01 00:00:00",
        "maximum_rows" -> "1000000"
      )
    val theRequest = HttpRequest(uri = Uri("https://hq.appsflyer.com/export/%s/%s/v5".format(appID, reportName)).withQuery(Query(params)))
    val responseFuture = Http().singleRequest(request = theRequest)

    responseFuture.map {
      response =>
        println(response.toString())
    }

  }
}
