package tripi.engine.dataProcess

import org.apache.commons.lang3.StringUtils.stripAccents
import org.apache.spark.sql.functions.udf
import scala.math.{E,pow}
import scala.util.matching.Regex

class MyUdf {

}
object MyUdf{
  val getHotelName = (str: String) => {
    var province = stripAccents(str)
    val patternDistrict = new Regex(".+(?=\\))")
    val patternHotel = new Regex("khach san")
    val patternMiniHotel = new Regex("nha nghi")
    val patternHomeStay = new Regex("nha dan")
    val patternDToD = new Regex("đ|ð")

    province = province.toLowerCase()
    province = patternHotel.replaceAllIn(province,"hotel")
    province = patternMiniHotel.replaceAllIn(province,"mini hotel")
    province = patternHomeStay.replaceAllIn(province,"home stay")
    province = patternDToD.replaceAllIn(province, "d")
    province = province.split(", ").last

    if(province.contains("(")){
      province = province.split("\\(").last
      province = patternDistrict.findAllIn(province).mkString
    }else{
    }
    province.trim()
  }

  val removeHTML = (str: String) => {
    if (str == null){
      ""
    }else{
      val data = str.replaceAll("""<(?!\/?a(?=>|\s.*>))\/?.*?>""", "")
      data.trim()
    }
  }

  val getProvince = (str: String) => {
    var province = stripAccents(str)
    val patternCountry = new Regex(".+(?=, viet)")
    val patternCommune = new Regex(".+(?=, xa)")
    val patternPhum = new Regex(".+(?=, phum banteai dek)")
    val patternPo = new Regex(".+(?=\\()")
    val patternDistrict = new Regex(".+(?=\\))")
    val patternHanoi = new Regex("hanoi")
    val patternHCM = new Regex("h chi minh")
    val patternSpace = new Regex("( -)")
    val patternSpot = new Regex("(\\.)")
    val patternDToD = new Regex("đ|ð")
    val patternSub =  new Regex("(tinh | province|thanh pho | district|khu vuc tp\\. |city| municipality)|s1-1516 |tp.|\\d")

    province = province.toLowerCase()
    province = patternHanoi.replaceAllIn(province,"ha noi")
    province = patternHCM.replaceAllIn(province,"ho chi minh")
    province = patternSub.replaceAllIn(province, "")
    province = patternSpace.replaceAllIn(province, "")
    province = patternDToD.replaceAllIn(province, "d")
    province = patternSpot.replaceAllIn(province, ", ")

    if(province.contains(", viet")){
      province = patternCountry.findAllIn(province).mkString
    }else{

    }

    if(province.contains(", xa trang bom")){
      province = patternCommune.findAllIn(province).mkString
    }else if(province.contains(", xa tam phuoc")){
      province = patternCommune.findAllIn(province).mkString
    }else if(province.contains(", phum banteai dek")){
      province = patternPhum.findAllIn(province).mkString
    }else{

    }

    province = province.split(", ").last

    if(province.contains("(")){
      if(province.contains("(tinh)") || province.contains("(va vung lan can)") || province.contains("(va vung phu can)") || province.contains("()")){
        province = patternPo.findAllIn(province).mkString
      }else{
        province = province.split("\\(").last
        province = patternDistrict.findAllIn(province).mkString
      }
    }else{

    }
    province.trim()
  }

  val getProvinceFromDistrict = (str: String) =>{

    str match {
      case "da lat" => "lam dong"
      case "dien bien phu" => "dien bien"
      case "ap thien phuoc" => "binh thuan"
      case "ba se" => "can tho"
      case "na ngo" => "lai chau"
      case "cam lam" => "khanh hoa"
      case "thon lac nghiep" | "phan rang" => "ninh thuan"
      case "huyen long khanh" => "dong nai"
      case "blao klong ner" => "lam dong"
      case "huyen phu quy" => "binh thuan"
      case "rach gia" => "kien giang"
      case "sa pa" => "lao cai"
      case "hoi an" | "yen khe" => "ninh binh"
      case "phuc yen" => "vinh phuc"
      case "tan tao" => "ho chi minh"
      case "cua lo" => "nghe an"
      case "hoang hoa" => "thanh hoa"
      case "gia lam pho" => "ha noi"
      case "ngoc quang" => "vinh phuc"
      case "nha trang" => "khanh hoa"
      case "ap phu" => "binh thuan"
      case "bac ha" => "lao cai"
      case "vung tau" | "xa thang tam" => "ba ria vung tau"
      case "moc chau" => "son la"
      case "vinh binh" => "binh duong"
      case "ban na ba" => "nghe an"
      case "dong ha" => "quang tri"
      case "an ma" => "bac kan"
      case "lao san chay" => "yen bai"
      case "ap da thien" => "lam dong"
      case "chau doc" => "an giang"
      case "phu quoc" => "kien gian"
      case "tu son" => "bac ninh"
      case "dien chau" => "nghe an"
      case _ => str
    }
  }

  val getJsonPrice = (hotelId: String,domainId: Long,date: String) => {
    val data = hotelId + "_" + domainId.toString + "_" +date
    data
  }

  val caculateService = (relax_spa: Int, relax_massage: Int, relax_outdoor_pool: Int, relax_sauna:Int, cleanliness_score: Float, meal_score: Float) =>{
    val relax_spa_score = if(relax_spa == -1) 0 else relax_spa
    val relax_massage_score = if(relax_massage == -1) 0 else relax_massage
    val relax_outdoor_pool_score = if(relax_outdoor_pool == -1) 0 else relax_outdoor_pool
    val relax_sauna_score = if(relax_sauna == -1) 0 else relax_sauna
    val cleanliness_score_score = if(cleanliness_score == -1) 0 else cleanliness_score
    val meal_score_score = if(meal_score == -1) 0 else meal_score

    val serviceScore = 1/(1+pow(E,-(relax_spa_score+relax_massage_score+relax_outdoor_pool_score+relax_sauna_score+cleanliness_score_score+meal_score_score)/30))

    serviceScore
  }

  val caculatePrice = (final_amount: Float) =>{
    val priceScore = 1/(1+pow(E,-final_amount/2000000))

    priceScore
  }

  val mapProvider = (url: String,domain: String,price: String) =>{
    val urlValue = if(url != null) url else ""
    val domainValue = if(domain != null) domain else ""
    val priceValue = if(price != null) price else ""
    val mapPrice = Map("provider"->domainValue,"price"->priceValue,"url"->urlValue)
    mapPrice
  }

  val convertPrice = (price: Float) =>{
    val formatter = java.text.NumberFormat.getIntegerInstance
    val priceString = formatter.format(price).toString
    priceString
  }

  // define udf
  val getProvinceUdf = udf(getProvince)

  val getProvinceFromDistrictUdf = udf(getProvinceFromDistrict)

  val getJsonPriceUdf = udf(getJsonPrice)

  val getHotelNameUdf = udf(getHotelName)

  val removeHTMLUdf = udf(removeHTML)

  val caculateServiceUdf = udf(caculateService)

  val caculatePriceUdf = udf(caculatePrice)

  val mapProviderUdf = udf(mapProvider)

  val convertPriceUdf = udf(convertPrice)
}