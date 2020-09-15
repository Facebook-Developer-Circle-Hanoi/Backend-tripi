package tripi.engine.dataProcess

import java.util.Calendar

import akka.actor.Actor
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.{col, count}
import tripi.engine.dataProcess.MyUdf.{caculateServiceUdf, getProvinceFromDistrictUdf, getProvinceUdf}

class DataProcessing {

  val config = ConfigFactory.load("/tmp/reference.conf")

  //Create a Spark session
  val spark = org.apache.spark.sql.SparkSession
    .builder()
    .config("spark.debug.maxToStringFields",100)
    .config("spark.cassandra.connection.host", "localhost")
    .master("local[*]")
    .appName("tripI - Data Processing")
    .getOrCreate()

  //Implicit methods available in Scala for converting common Scala objects into DataFrames
  import spark.implicits._

  //Get Spark Context from Spark session
  val sparkContext = spark.sparkContext

  //Set the Log file level
  sparkContext.setLogLevel("WARN")

  val dataMap = new DataMapping
  /**
   * Get data form click house and save to Cassandra
   * @return no
   * */
  def dataProcessing(): Unit ={
    // read hotel data of other domain
    val otherHotel = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://13.229.139.134:8123/PhoeniX?charset=utf8")
      .option("dbtable", "hotel_mapping")
      .option("user", "Micro")
      .option("password", "z4VPkWHOy6XpcBEkOGI7")
      .load()

    // read hotel data url
    val infoHotel = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://13.229.139.134:8123/PhoeniX?charset=utf8")
      .option("dbtable", "hotel_info")
      .option("user", "Micro")
      .option("password", "z4VPkWHOy6XpcBEkOGI7")
      .load()

    // read hotel data price
    val hotelPriceDaily = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://13.229.139.134:8123/PhoeniX?charset=utf8")
      .option("dbtable", "hotel_price_daily")
      .option("user", "Micro")
      .option("password", "z4VPkWHOy6XpcBEkOGI7")
      .load()

    // read hotel data of TripI
    val tripIHotel = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://13.229.139.134:8123/PhoeniX?charset=utf8")
      .option("dbtable", "roothotel_info")
      .option("user", "Micro")
      .option("password", "z4VPkWHOy6XpcBEkOGI7")
      .load()

    // read province data
    val province = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://13.229.139.134:8123/PhoeniX?charset=utf8")
      .option("dbtable", "province")
      .option("user", "Micro")
      .option("password", "z4VPkWHOy6XpcBEkOGI7")
      .load()

    // read domain data
    val domain = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://13.229.139.134:8123/PhoeniX?charset=utf8")
      .option("dbtable", "domain")
      .option("user", "Micro")
      .option("password", "z4VPkWHOy6XpcBEkOGI7")
      .load()

    // read service data
    val service = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://13.229.139.134:8123/PhoeniX?charset=utf8")
      .option("dbtable", "hotel_service")
      .option("user", "Micro")
      .option("password", "z4VPkWHOy6XpcBEkOGI7")
      .load()

    // read quality data
    val quality = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://13.229.139.134:8123/PhoeniX?charset=utf8")
      .option("dbtable", "hotel_quality")
      .option("user", "Micro")
      .option("password", "z4VPkWHOy6XpcBEkOGI7")
      .load()

    // read quality data
    val review = spark.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://13.229.139.134:8123/PhoeniX?charset=utf8")
      .option("dbtable", "hotel_review")
      .option("user", "Micro")
      .option("password", "z4VPkWHOy6XpcBEkOGI7")
      .load()

    val reviewCount = review.groupBy(col("domain_id"),col("domain_hotel_id")).agg(
      count("review_id").as("review")
    )

    val infoHotelData = infoHotel.select(col("domain_hotel_id").cast("Long"),
      col("domain_id").cast("Int"),
      col("url").cast("String"))

    val serviceData = service.select(col("hotel_id").cast("String"),
        col("night_club").cast("Int"),
        col("relax_spa").cast("Int"),
        col("relax_massage").cast("Int"),
        col("relax_steam_room").cast("Int"),
        col("relax_outdoor_pool").cast("Int"),
        col("relax_sauna").cast("Int"))

    val qualityData = quality.select(col("hotel_id").cast("String"),
        col("cleanliness_score").cast("Float"),
        col("meal_score").cast("Float"))

    val hotelPriceDailyData = hotelPriceDaily.select(col("domain_id"),
        col("domain_hotel_id").cast("BigInt"),
        col("final_amount_min").cast("Float"))

    val otherHotelData = otherHotel.select(col("id").cast("String").as("hotel_id"),
        col("domain_hotel_id").cast("BigInt"),
        col("domain_id").cast("Int"),
        col("name").cast("String"),
        col("address").cast("String"),
        col("star_number").cast("Int"),
        col("overall_score").cast("Float"),
        col("checkin_time").cast("String"),
        col("checkout_time").cast("String"))
      .filter(col("address") isNotNull)
      .dropDuplicates("domain_id","domain_hotel_id")

    val otheHotel1 = otherHotelData.join(infoHotelData,Seq("domain_id","domain_hotel_id"),"left")
      .join(hotelPriceDailyData,Seq("domain_id","domain_hotel_id"),"left")


    val otherHotelDataDomain = otheHotel1.select(col("hotel_id"),
    col("domain_id"),
    col("domain_hotel_id"))

    val otherHotelService = serviceData
      .join(qualityData,Seq("hotel_id"),"left")
      .join(otherHotelDataDomain,Seq("hotel_id"),"left")
      .filter(col("hotel_id") isNotNull)

    val dataService = otherHotelService.withColumn("scoreservice",caculateServiceUdf(col("relax_spa"),
      col("relax_massage"),
      col("relax_outdoor_pool"),
      col("relax_sauna"),
      col("cleanliness_score"),
      col("meal_score")).cast("Float"))

    val otherHotelDataAddProvince = otheHotel1.select(col("hotel_id").cast("String"),
      col("domain_hotel_id").cast("BigInt"),
      col("domain_id").cast("Int"),
      col("name").cast("String"),
      col("final_amount_min").cast("Float"),
      col("address").cast("String"),
      col("url").cast("String"),
      col("star_number").cast("Int"),
      col("overall_score").cast("Float"),
      col("checkin_time").cast("String"),
      col("checkout_time").cast("String"),
      getProvinceUdf(col("address")).cast("String").as("province"))
      .filter(col("province") =!= "trung quoc")

    val otherHotelDataAddProvinceClean = otherHotelDataAddProvince.select(col("hotel_id").cast("String"),
      col("domain_hotel_id").cast("BigInt"),
      col("domain_id").cast("Int"),
      col("name").cast("String"),
      col("final_amount_min").cast("Float"),
      col("address").cast("String"),
      col("url").cast("String"),
      col("star_number").cast("Int"),
      col("overall_score").cast("Float"),
      col("checkin_time").cast("String"),
      col("checkout_time").cast("String"),
      getProvinceFromDistrictUdf(col("province")).as("province"))
    otherHotelDataAddProvince.unpersist()
    otheHotel1.unpersist()
    val provinceData = province.select(
      col("id").as("province_id"),
      col("name_no_accent").cast("String").as("province")
    )

    val domain_data = domain.select(
      col("id").as("domain_id"),
      col("name").cast("String").as("domain")
    )

    val groupByProvinceData = otherHotelDataAddProvinceClean
      .join(provinceData,Seq("province"),"left")

    val groupByProvinceDataFilter = groupByProvinceData.filter(col("province") isNotNull)

    val dataMapping = groupByProvinceDataFilter.join(domain_data,Seq("domain_id"),"left")
      .join(reviewCount,Seq("domain_id","domain_hotel_id"),"left")
      .na.fill(0)

    dataMapping.filter(col("province_id") isNotNull)
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "hotel_other", "keyspace" -> "trip"))
      .save()

    tripIHotel.filter(col("province_id") isNotNull)
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "hotel_tripi", "keyspace" -> "trip"))
      .save()

    dataService
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "hotel_service", "keyspace" -> "trip"))
      .save()

    provinceData
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "province", "keyspace" -> "trip"))
      .save()

    domain_data
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "domain", "keyspace" -> "trip"))
      .save()
    println(Calendar.getInstance().getTime + ": data processing is success\n")

    // remove data frame from cahe
    groupByProvinceData.unpersist()
    otherHotelDataAddProvinceClean.unpersist()
    provinceData.unpersist()

    // start Mapping process
    dataMap.mapping()
  }
}


case object dataProcessing

//Define RealtimeProcessing actor
class dataProcessingActor(DataProcessing: DataProcessing) extends Actor{

  //Implement receive method
  def receive = {
    //Start hashtag realtime processing
    case dataProcessing => {
      println(Calendar.getInstance().getTime + ": Start data processing...\n")
      DataProcessing.dataProcessing()
    }
  }

}