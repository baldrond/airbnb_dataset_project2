import java.io.{File, PrintWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object alternative_listings {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val listing_id = args(0).trim()
    val date = args(1).trim()
    val price = args(2).trim().toDouble
    val distance = args(3).trim().toDouble
    val n = args(4).trim().toInt
    val params = listing_id+"_"+date+"_"+price+"_"+distance+"_"+n

    val listings = sc.textFile("..\\airbnb_data\\listings_us.csv")
    val calendar = sc.textFile("..\\airbnb_data\\calendar_us.csv")

    val listingsData = listings.map(line => line.split("\t")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    val calendarData = calendar.map(line => line.split("\t")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val chosenListing = listingsData.filter(row => row(43).equals(listing_id)).map(row => (row(43), (row(81), row(65).replaceAll("[,$]", "").toDouble, row(2).replaceAll("[{}\"]", "").split(","), (row(54).toDouble, row(51).toDouble)))).collect()

    //81 <- roomtype, 65 <- price, 2 <- ameneties, 54 <- long, 51 <- lat, 43 <- id, 60 <- name
    val listingmap = listingsData.map(row => (row(43), (row(81), row(65).replaceAll("[,$]", "").toDouble, row(2).replaceAll("[{}\"]", "").split(","), haversine(chosenListing(0)._2._4._2, chosenListing(0)._2._4._1, row(51).toDouble, row(54).toDouble), row(60))))
    val calendarmap = calendarData.map(row => (row(0),(row(1), row(2))))

    val roomtype_filter = listingmap.filter(row => row._2._1.equals(chosenListing(0)._2._1))

    val price_filter = roomtype_filter.filter(row => row._2._2 <= chosenListing(0)._2._2*((price/100)+1))

    val distance_filter = price_filter.filter(row => row._2._4 <= distance)

    val calendar_filter = calendarmap.filter(row => row._2._1.equals(date) && row._2._2.equals("t"))
    val calendar_join = distance_filter.join(calendar_filter)

    val final_map = calendar_join.map(row => (count_common_ameneties(row._2._1._3, chosenListing(0)._2._3),(row._1, row._2._1._5, row._2._1._4 ,row._2._1._2)))

    sc.parallelize(final_map.top(n)).map(row => row._2._1+"\t"+row._2._2+"\t"+row._1+"\t"+row._2._3+"\t"+row._2._4).coalesce(1).saveAsTextFile(params+"_result.tsv")

    //For creating the files for Carto
    //createCartoFiles(listing_id, listingsData, calendar_join, chosenListing(0)._2._3)
  }

  def createCartoFiles(listing_id : String, listingsData : RDD[Array[String]], calendar_join : RDD[((String),((String, Double, Array[String], Double, String), (String, String)))], chosen_listing_ameneties : Array[String]) : Unit ={
    val carto = listingsData.map(row => (row(43),(row(51), row(54), row(64), row(79), row(65))))
    val printCarto = calendar_join.join(carto).map(row => row._2._1._1._5+"\t"+row._2._2._5+"\t"+row._2._2._1+"\t"+row._2._2._2+"\t"+row._2._2._3+"\t"+Math.round((row._2._1._1._4)*100)/100.0+" km\t"+matching_amen(row._2._1._1._3, chosen_listing_ameneties)).collect()

    val pw1 = new PrintWriter(new File("carto_data.tsv"))
    pw1.write("name\tprice\tlat\tlon\tpicture-url\tdistance to original listing\tcommon ameneties\n")
    for(line <- printCarto){
      pw1.write(line)
    }
    pw1.close()

    val printOriginal = listingsData.filter(row => row(43).equals(listing_id)).map(row => row(60)+"\t"+row(65)+"\t"+row(51)+"\t"+row(54)+"\t"+row(64)+"\t"+row(2).replaceAll("[{}\"]", ""))
    val pw2 = new PrintWriter(new File("carto_original.tsv"))
    pw2.write("name\tprice\tlat\tlon\tpicture-url\tameneties\n")
    pw2.write(printOriginal.first())
    pw2.close()
  }

  def count_common_ameneties(ameneties1 : Array[String], ameneties2 : Array[String]) : Int ={
    var count = 0
    for(a <- ameneties1){
     if(ameneties2.contains(a)){
       count += 1
      }
    }
    return count
  }
  def matching_amen(ameneties1 : Array[String], ameneties2 : Array[String]) : String ={
    var list = ""
    for(a <- ameneties1){
      if(ameneties2.contains(a)){
        list += a+", "
      }
    }
    if(list.length >= 3) {
      return list.substring(0, list.length - 2)
    } else {
      return ""
    }
  }

  def haversine(lat1 : Double, lon1 : Double, lat2 : Double, lon2 : Double) : Double ={
    val la1 = Math.toRadians(lat1)
    val lo1 = Math.toRadians(lon1)
    val la2 = Math.toRadians(lat2)
    val lo2 = Math.toRadians(lon2)

    val dlon = lo2 - lo1
    val dlat = la2 - la1

    val a = Math.pow(Math.sin(dlat/2),2) + Math.cos(la1) * Math.cos(la2) * Math.pow(Math.sin(dlon/2),2)

    val c = 2 * Math.asin(Math.sqrt(a))

    val r = 6371

    return r * c
  }
}
