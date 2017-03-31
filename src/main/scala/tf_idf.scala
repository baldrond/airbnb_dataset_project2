import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import spray.json._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object tf_idf {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val path = args(0).trim()
    val param = args(1).trim()
    val id_or_hood = args(2).trim()
    val params = param+"_"+id_or_hood

    val listings = sc.textFile(path+"\\listings_us.csv")
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

    //Case-classes for reading the GeoJSON-file.
    case class Properties(
                           neighbourhood: String,
                           neighbourhood_group: Option[String]
                         )
    case class Geometry(
                         coordinates: Seq[Seq[Seq[Seq[Double]]]],
                         `type`: String
                       )

    case class GeoJSON(
                        properties: Properties,
                        geometry: Geometry,
                        `type`: String
                      )

    case class Features(
                         features: Seq[GeoJSON],
                         `type`: String
                       )

    object MyJsonProtocol extends DefaultJsonProtocol {
      implicit val propertiesFormat = jsonFormat2(Properties)
      implicit val geometryFormat = jsonFormat2(Geometry)
      implicit val geojsonFormat = jsonFormat3(GeoJSON)
      implicit val featuresFormat = jsonFormat2(Features)
    }

    import MyJsonProtocol._

    val input = scala.io.Source.fromFile(path+"\\neighbourhoods.geojson")("UTF-8").mkString.parseJson

    val jsonCollection = input.convertTo[Features]

    val features = jsonCollection.features

    var neighbourhood_list = new ListBuffer[(String, Array[String])]()

    val listingmap = listingsData.map(row => ((row(54).toDouble, row(51).toDouble), row(43), row(19).toLowerCase()
      .replaceAll("[,.!?:)(/\"]"," ")
      .split(" ")
      .filterNot(_.isEmpty)))

    for (aListing <- listingmap.collect()) {
      val point = new Point(aListing._1._1, aListing._1._2)
      var foundNeighborhood = ""
      var foundNeighborhood_group = ""
      for (geojson <- features) {
        val name = geojson.properties.neighbourhood
        val group = geojson.properties.neighbourhood_group
        var group_string = ""
        if (!group.isEmpty) {
          group_string = group.get
        }
        if (ID.equalsIgnoreCase(name) || ID.equalsIgnoreCase(group_string)) {
          val coords0 = geojson.geometry.coordinates
          val allEdges = new ArrayBuffer[Edge]
          var longmax = -99999.0
          var latmax = -99999.0
          //Goes through all coordinates
          for (coords1 <- coords0) {
            for (coords2 <- coords1) {
              val pstart = new Point(coords2(0)(0), coords2(0)(1))
              var pforrige = new Point(0, 0)
              for (coords3 <- coords2) {
                //Creates an edge for each points after each other in array.
                val p = new Point(coords3(0), coords3(1))
                if (pforrige.lat != 0 && pforrige.long != 0) {
                  val edge = new Edge(pforrige, p)
                  allEdges += edge
                }
                pforrige = p
                //Store largest longitude and latitude

                longmax = Math.max(longmax, coords3(0))
                latmax = Math.max(latmax, coords3(1))
              }
              //Creates an edge between the first and the last point.
              val edge = new Edge(pforrige, pstart)
              allEdges += edge
            }
          }
          //A point definitly outside of the polygon.
          val outsidePoint = new Point(longmax + 0.01, latmax + 0.01)
          val edge = new Edge(point, outsidePoint)
          val polygon = new Polygon(allEdges.toSeq)
          val num = polygon.checkCollisions(edge)
          //Get number of collisions between edge and polygon. If odd number -> Right neighborhood
          if (num % 2 != 0) {
            foundNeighborhood = name
            //Some lisings finds no group. Checks if it has a group before setting one
            foundNeighborhood_group = group_string
          }
        }
      }
      //Add all listing  to a list with id as a key
      if (!foundNeighborhood.equals("")) {
        val entry = (aListing._2, aListing._3)
        neighbourhood_list += entry
      }
    }
    val neighbourhoodRDD = sc.parallelize(neighbourhood_list)

    val flatmap = neighbourhoodRDD.flatMap(row => row._2)

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
