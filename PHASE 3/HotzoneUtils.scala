package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    	val coordinates_rect = queryRectangle.split(",")
        val x1 = coordinates_rect(0).toDouble
        val y1 = coordinates_rect(1).toDouble
        val x2 = coordinates_rect(2).toDouble
        val y2 = coordinates_rect(3).toDouble

        var maxx = 0.0
        var minx = 0.0
        var maxy = 0.0
        var miny = 0.0

        if( x1 > x2 ){
            maxx = x1
            minx = x2
        } else {
            maxx = x2
            minx = x1
        }
        if( y1 > y2 ){
            maxy = y1
            miny = y2
        } else {
            maxy = y2
            miny = y1
        } 

        val coordinates_point = pointString.split(",")
        val pointx = coordinates_point(0).toDouble
        val pointy = coordinates_point(1).toDouble

        if(pointx >= minx && pointx <= maxx && pointy >= miny && pointy <= maxy ) {
            true
        } else {
            false
        }
  }

  // YOU NEED TO CHANGE THIS PART

}
