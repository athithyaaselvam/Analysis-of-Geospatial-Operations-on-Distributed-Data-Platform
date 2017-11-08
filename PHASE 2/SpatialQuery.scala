package cse512

import org.apache.spark.sql.SparkSession
import math.{ sqrt, pow }

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((
    {
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

    )))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((

    {
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





    )))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((
    {
        val coordinates_point1 = pointString1.split(",")
        val x1 = coordinates_point1(0).toDouble
        val y1 = coordinates_point1(1).toDouble
        
        val coordinates_point2 = pointString2.split(",")
        val x2 = coordinates_point2(0).toDouble
        val y2 = coordinates_point2(1).toDouble

        var myDistance = sqrt(pow(x1 - x2, 2) + pow(y1 - y2, 2))

        if(myDistance < distance) {
            true
        } else {
            false
        }
    }
    )))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((
    {
        val coordinates_point1 = pointString1.split(",")
        val x1 = coordinates_point1(0).toDouble
        val y1 = coordinates_point1(1).toDouble
        
        val coordinates_point2 = pointString2.split(",")
        val x2 = coordinates_point2(0).toDouble
        val y2 = coordinates_point2(1).toDouble

        var myDistance = sqrt(pow(x1 - x2, 2) + pow(y1 - y2, 2))

        if(myDistance < distance) {
            true
        } else {
            false
        }
    }
    )))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
