package tripi.engine.dataProcess

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Dataset, Encoders, Row}
import org.apache.spark.sql.functions.{asc, col, desc}
import scala.collection.JavaConversions._
import org.apache.commons.lang3.StringUtils.{ stripAccents}
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import scala.util.matching.Regex

class ServingProcessing {}

object ServingProcessing {
  // logfile for spark
  val config = ConfigFactory.load("/tmp/reference.conf")

  //Create a Spark session
  val spark = org.apache.spark.sql.SparkSession
    .builder()
    .config("spark.debug.maxToStringFields",100)
    .config("spark.cassandra.connection.host", "localhost")
    .master("local[*]")
    .appName("tripI - Batch Processing")
    .getOrCreate()

  //Implicit methods available in Scala for converting common Scala objects into DataFrames
  import spark.implicits._

  //Get Spark Context from Spark session
  val sparkContext = spark.sparkContext

  //Set the Log file level
  sparkContext.setLogLevel("WARN")

  case class hotel(id: String,
                   provider: String,
                   name: String,
                   address: String,
                   price: String,
                   rank: Float,
                   review: Int,
                   star_number: Int,
                   overall_score: Float,
                   checkin_time: String,
                   checkout_time: String,
                   suggest: Array[Map[String,String]])

  def getData(x: Option[Any]) = x match {
    case Some(s) => {
      if(s == null){
        "0"
      }else{
        s.toString
      }
    }
    case None => "0"
    case _ => "0"
  }
  // get proven form address

  class readData(val hotelInfo:Dataset[Row],val suggest:Dataset[Row]){

    def search (page:Int,key:String) : readData = {
      val patternDToD = new Regex("đ|ð")
      var newkey = patternDToD.replaceAllIn(key, "d")
      newkey = stripAccents(newkey)

      newkey = newkey.toLowerCase().trim
      val dataHotel = this.hotelInfo
        .filter(col("province") ===(newkey))
      val dataHotelRank = dataHotel.orderBy(desc("rank"))
      val offset = dataHotelRank.limit(page*5)
      val data = dataHotelRank.except(offset).limit(5).join(this.suggest,Seq("id"))
      val readdata = new readData(data,this.suggest)

      readdata
    }

    /**
     * convert data frame to json
     * @return String
     * */
    def load ():  String = {
      val schema = Encoders.product[hotel]
      org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)

      // convert data frame to list of case class
      val data = this.hotelInfo
        .na.drop()
        .as[hotel](schema)
        .collectAsList
        .toList
      val jsonString = Json(DefaultFormats).write(data)

      // remove data frame from cache
      this.hotelInfo.unpersist()
      jsonString
    }
  }

  /**
   * Read data form cassandra to data frame.
   * @return readData class
   * */
  def readData () : readData = {
    // Get data
    val hotel_info = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "hotel_info", "keyspace" -> "trip"))
      .load()
      .cache()
      .filter(col("price").cast("String")=!=("0"))

    val dataHotal = hotel_info.select(col("id"),
      col("provider"),
      col("name"),
      col("province"),
      col("rank"),
      col("review"),
      col("address"),
      col("star_number"),
      col("overall_score"),
      col("checkin_time"),
      col("checkout_time"),
      col("price"))

    val suggest = hotel_info.select(col("id"),
      col("suggest"))

    val readdata = new readData(dataHotal,suggest)
    readdata
  }
}
