package tripi.engine

import akka.actor.{ActorSystem, Props}
import tripi.engine.dataProcess.{DataMapping, DataProcessing, ServingProcessing, dataProcessing, dataProcessingActor}
import tripi.engine.servingLayer.Server

import scala.concurrent.duration.DurationInt

object Main {
  def main(args: Array[String]): Unit = {
    //val actorSystem = ActorSystem("ActorSystem")

    //val dataProcess = new DataProcessing

    //val dataActor = actorSystem.actorOf(Props(new dataProcessingActor(dataProcess)))

    // set time for process
    //import actorSystem.dispatcher
    //val initialDelay = 100 milliseconds
    //val batchInterval= 48 hours

    // start process
    //actorSystem.scheduler.schedule(initialDelay,batchInterval,dataActor,dataProcessing)

    // start server
    val server = new Server
    server.start()
   // val readData = ServingProcessing
   // val hotel_info = readData.readData("hotel_info","hotel_service","hotel_mapping")
      //.getPrice(1,"ha noi")
    //val datamapping = new DataMapping
    //datamapping.mapping()
  }
}
