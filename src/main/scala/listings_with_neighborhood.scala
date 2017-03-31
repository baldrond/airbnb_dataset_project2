import java.io.{File, PrintWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import spray.json.DefaultJsonProtocol
import spray.json._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object listings_with_neighborhood {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val path = args(0).trim()

    val listings = sc.textFile(path+"\\listings_us.csv").map(line => line.split("\t"))

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

    var neighbourhood_list = new ListBuffer[Array[String]]()

    val collected_listings = listings.collect()

    for (aListing <- collected_listings) {
      if (aListing != collected_listings.head) {
        val point = new Point(aListing(54).toDouble, aListing(51).toDouble)
        var foundNeighborhood = ""
        var foundNeighborhood_group = ""
        for (geojson <- features) {
          val name = geojson.properties.neighbourhood
          val group = geojson.properties.neighbourhood_group
          var group_string = ""
          if (!group.isEmpty) {
            group_string = group.get
          }
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
        //Add all listing
        val entry = (aListing :+ foundNeighborhood :+ foundNeighborhood_group)
        neighbourhood_list += entry
      } else {
        val entry = (aListing :+ "neighborhood" :+ "neighborhood_group")
        neighbourhood_list += entry
      }
    }
    val pw = new PrintWriter(new File(path+"\\listings_with_neighborhood.csv"))
    for(line <- neighbourhood_list){
      val tabbed_line = line.mkString("\t")
      pw.write(tabbed_line+"\n")
    }
    pw.close()
  }
}
