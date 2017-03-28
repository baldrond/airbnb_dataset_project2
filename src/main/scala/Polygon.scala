/*
Class to define a polygon from the geofiles.
Has a method to check how many collisions an edge will have with the polygon.
 */

class Polygon(var edges: Seq[Edge]){

  def checkCollisions(e: Edge): Int = {
    var collisions = 0
    for(edge <- edges){
      if(e.collides(edge)){
        collisions+=1
      }
    }
    collisions
  }
}