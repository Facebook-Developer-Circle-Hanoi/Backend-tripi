package tripi.engine.servingLayer

import java.util.Calendar

import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials, RawHeader}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives.{get, _}

import scala.io.StdIn
import tripi.engine.dataProcess.ServingProcessing
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await}

class Server {
  val readData = ServingProcessing

  // Define implicit variables
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  def start() {
    val route: Route = concat(
        get {
          path("serch") {
            parameters('page.as[Int], 'key.as[String]) { (page, key) =>
              complete {
                val hotel_info = readData.readData()
                val getprice =  hotel_info.search(page,key).load()
                getprice
              }
            }
          }
      }
    )

    //Binding to the host and port
    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)
    println(Calendar.getInstance().getTime + s": Server online at http://localhost:8080/\nPress Enter to stop...\n")
    StdIn.readLine() // let the server run until user presses Enter

    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ ⇒ system.terminate()) // and shutdown when done
  }
}
