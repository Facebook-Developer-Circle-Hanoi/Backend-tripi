package tripi.engine.dataProcess

import java.util.Calendar

import org.apache.spark.sql.functions.{collect_list, first, min}
import tripi.engine.dataProcess.MyUdf.{convertPriceUdf, getHotelNameUdf, mapProviderUdf, removeHTMLUdf}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.{col, levenshtein}
import tripi.engine.dataProcess.MyUdf.caculatePriceUdf

class DataMapping{
  val config = ConfigFactory.load("/tmp/reference.conf")

  //Create a Spark session
  val spark = org.apache.spark.sql.SparkSession
    .builder()
    .config("spark.debug.maxToStringFields",100)
    .config("spark.cassandra.connection.host", "localhost")
    .master("local[*]")
    .appName("tripI - Mapping Processing")
    .getOrCreate()

  //Implicit methods available in Scala for converting common Scala objects into DataFrames
  import spark.implicits._

  //Get Spark Context from Spark session
  val sparkContext = spark.sparkContext

  //Set the Log file level
  sparkContext.setLogLevel("WARN")

  /**
   * Mapping tripI hotel with ether domain hotel and save to Cassandra
   * @return no
   * */
  def mapping(): Unit ={
    println(Calendar.getInstance().getTime + ": Start data mapping...\n")

    val tripiData = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "hotel_tripi", "keyspace" -> "trip"))
      .load()

    val otherData = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "hotel_other", "keyspace" -> "trip"))
      .load()

    val hotel_service = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "hotel_service", "keyspace" -> "trip"))
      .load()

    val otherDataAddRank = otherData.join(hotel_service,Seq("hotel_id","domain_hotel_id","domain_id"),"left")
      .withColumn("priceScore",caculatePriceUdf(col("final_amount_min")))

    val otherDataEnd = otherDataAddRank
      .withColumn("rank",col("scoreservice")/col("priceScore"))

    for(province_id <- 1 to 63){
      val tripIHotelData = tripiData.filter(col("province_id").cast("Int") === province_id )
      val otherHotelData = otherDataEnd.filter(col("province_id").cast("Int") === province_id )

      val otherHotelDataNomal = otherHotelData.select(col("hotel_id"),
        col("domain_hotel_id"),
        col("domain_id"),
        col("domain"),
        col("rank"),
        col("url"),
        col("review"),
        convertPriceUdf(col("final_amount_min")).as("final_amount_min"),
        getHotelNameUdf(col("name")).as("nameOther"),
        col("star_number").as("star_number_other"),
        col("overall_score").as("overall_score_other").cast("Float"),
        col("province"))
        .na.fill(0)

      val TripIHotelDataNomal = tripIHotelData.select(col("id").cast("String"),
        col("province_id"),
        getHotelNameUdf(col("name")).as("name"),
        col("address"),
        col("star_number"),
        col("overall_score"),
        col("checkin_time"),
        col("checkout_time"),
        removeHTMLUdf(col("description")).as("description"))
        .na.fill(0)

      val groupByName = TripIHotelDataNomal
        .join(otherHotelDataNomal, levenshtein(TripIHotelDataNomal("name"), otherHotelDataNomal("nameOther")) < 1)

      val groupById = groupByName.withColumn("suggest",mapProviderUdf(col("url"),col("domain"),col("final_amount_min")))

      val groupByIdPrice = groupById.groupBy("id").agg(
        first(col("domain")).as("provider"),
        first(col("province")).as("province"),
        first(col("name")).as("name"),
        first(col("rank")).as("rank"),
        first(col("review")).as("review"),
        first(col("address")).as("address"),
        first(col("star_number")).as("star_number"),
        first(col("overall_score")).as("overall_score"),
        first(col("checkin_time")).as("checkin_time"),
        first(col("checkout_time")).as("checkout_time"),
        min(col("final_amount_min")).as("price"),
        collect_list("suggest").as("suggest")
      )
      groupByIdPrice
        .write
        .format("org.apache.spark.sql.cassandra")
        .mode("Append")
        .options(Map("table" -> "hotel_info", "keyspace" -> "trip"))
        .save()
      print(".")
    }
    print(Calendar.getInstance().getTime + ": Mapping process is success\n")
  }
}