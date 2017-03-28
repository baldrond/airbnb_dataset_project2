/*
Class to define an edge between to points
Has a method to check if the edge collides with another edge:
  Checks if each point of an edge is on opposite side of the other edge and visa versa.
 */

class Edge(var p1: Point, var p2: Point){
  def collides(e: Edge): Boolean = {
    val t11 = (p1.long - p2.long)*(e.p1.lat - p1.lat) - (p1.lat - p2.lat)*(e.p1.long - p1.long)
    val t12 = (p1.long - p2.long)*(e.p2.lat - p1.lat) - (p1.lat - p2.lat)*(e.p2.long - p1.long)
    val t21 = (e.p1.long - e.p2.long)*(p1.lat - e.p1.lat) - (e.p1.lat - e.p2.lat)*(p1.long - e.p1.long)
    val t22 = (e.p1.long - e.p2.long)*(p2.lat - e.p1.lat) - (e.p1.lat - e.p2.lat)*(p2.long - e.p1.long)

    if((t11 >= 0  && t12 < 0) || (t12 >= 0  && t11 < 0)){
      if((t21 >= 0  && t22 < 0) || (t22 >= 0  && t21 < 0)){
        true
      } else {
        false
      }
    } else {
      false
    }
  }
}
