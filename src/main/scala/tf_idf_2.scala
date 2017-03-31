import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object tf_idf_2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val path = args(0).trim()
    val param = args(1).trim()
    val id_or_hood = args(2).trim()
    val params = param+"_"+id_or_hood

    val listings = sc.textFile(path+"\\listings_with_neighborhood.csv")
    val listingsData = listings.map(line => line.split("\t")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val all_listings = listingsData.map(row => (row(43), row(19).toLowerCase()
      .replaceAll("[,.!?:)(/\"]"," ")
      .split(" ")
      .filterNot(_.isEmpty)))

    var number_of_appearance = sc.emptyRDD[(String, Int)]
    var number_of_terms = 0L
    if(param.equals("-l")){
      val input = withID(id_or_hood, all_listings)
      number_of_appearance = input._1
      number_of_terms = input._2
    } else if(param.equals("-n")){
      val input = withNeighborhood(id_or_hood, listingsData, sc, path)
      number_of_appearance = input._1
      number_of_terms = input._2
    } else {
      println("Wrong parameter. Exit program")
      System.exit(0)
    }

    val number_of_documents = all_listings.count();

    val all_listings_collected = all_listings.collect()

    val word_tf_idf = new ListBuffer[(Double, String)]()

    for(word <- number_of_appearance.collect()) {
      var word_count_in_all_docs = 0
      for (aListing <- all_listings_collected) {
        if(aListing._2.contains(word._1))
          word_count_in_all_docs += 1
      }

      val tf = 1.0 * word._2 / number_of_terms
      val idf = 1.0 * number_of_documents / word_count_in_all_docs

      val tf_idf = tf * idf
      val entry = (tf_idf, word._1)
      word_tf_idf += entry
    }

    val word_tf_idf_RDD = sc.parallelize(word_tf_idf)

    sc.parallelize(word_tf_idf_RDD.top(100)).map(row => row._2+","+row._1).coalesce(1).saveAsTextFile(path+"/"+params+"_tf_idf.csv")

  }

  def withNeighborhood(ID: String, listingsData : RDD[Array[String]], sc: SparkContext, path: String): (RDD[(String, Int)], Long) ={

    val listingmap = listingsData.filter(row => row(row.length-1).equalsIgnoreCase(ID) || row(row.length-2).equalsIgnoreCase(ID)).map(row => ((row(54).toDouble, row(51).toDouble), row(43), row(19).toLowerCase()
      .replaceAll("[,.!?:)(/\"]"," ")
      .split(" ")
      .filterNot(_.isEmpty)))
    println(listingmap.count())

    val flatmap = listingmap.flatMap(row => row._3)

    val number_of_terms = flatmap.count()
    val number_of_appearance = flatmap.map(row => (row, 1)).reduceByKey((a,b) => a + b)

    return (number_of_appearance, number_of_terms)

  }

  def withID(ID: String, all_listings : RDD[(String, Array[String])]): (RDD[(String, Int)], Long) ={

    val flatmap = all_listings.filter(row => row._1.equalsIgnoreCase(ID)).flatMap(row => row._2)

    var number_of_appearance = flatmap.map(row => (row, 1)).reduceByKey((a,b) => a + b)

    var number_of_terms = flatmap.count()

    return (number_of_appearance, number_of_terms)
  }
}
