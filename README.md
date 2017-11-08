# Big-Data-Prediction---NYC-taxi-database

Load GeoSpark jar into Apache Spark Scala shell

PHASE 1:

1. Create GeoSpark SpatialRDD (PointRDD).

2. Spatial Range Query: Query the PointRDD using this query window [x1(35.08),y1(-113.79),x2(32.99),y2(-109.73)].
  a. Query the PointRDD
  b. Build R-Tree index on PointRDD then query this PointRDD.

3. Spatial KNN query: Query the PointRDD using this query point [x1(35.08),y1(-113.79)].
  a. Query the PointRDD and find 5 Nearest Neighbors.
  b. Build R-Tree index on PointRDD then query this PointRDD again.

4. Spatial Join query: Create a GeoSpark RectangleRDD and use it to join PointRDD
  a. Join the PointRDD using Equal grid without R-Tree index.
  b. Join the PointRDD using Equal grid with R-Tree index.
  c. Join the PointRDD using R-Tree grid without R-Tree index.


PHASE 2:

write two User Defined Functions ST_Contains and ST_Within in SparkSQL and use them to do four spatial queries:

1. Range query: Use ST_Contains. Given a query rectangle R and a set of points P, find all the points within R.

2. Range join query: Use ST_Contains. Given a set of Rectangles R and a set of Points S, find all (Point, Rectangle) pairs        such that the point is within the rectangle.

3. Distance query: Use ST_Within. Given a point location P and distance D in km, find all points that lie within a distance D    from P

4. Distance join query: Use ST_Within. Given a set of Points S1 and a set of Points S2 and a distance D in km, find all (s1,      s2) pairs such that s1 is within a distance D from s2 (i.e., s1 belongs to S1 and s2 belongs to S2).


The detailed requirements are as follows:

1. ST_Contains

Input: pointString:String, queryRectangle:String

Output: Boolean (true or false)

Definition: You first need to parse the pointString (e.g., "-88.331492,32.324142") and queryRectangle (e.g., "-155.940114,19.081331,-155.618917,19.5307") to a format that you are comfortable with. Then check whether the queryRectangle fully contains the point. Consider on-boundary point.

2. ST_Within

Input: pointString1:String, pointString2:String, distance:Double

Output: Boolean (true or false)

Definition: You first need to parse the pointString1 (e.g., "-88.331492,32.324142") and pointString2 (e.g., "-88.331492,32.324142") to a format that you are comfortable with. Then check whether the two points are within the given distance. Consider on-boundary point. To simplify the problem, please assume all coordinates are on a planar space and calculate their Euclidean distance.

3. Use Your UDF in SparkSQL

The code template has loaded the original data (point data, arealm.csv, and rectangle data, zcta510.csv) into DataFrame using tsv format. You don't need to worry about the loading phase.

Range query:

  select * from point where ST_Contains(point._c0,'-155.940114,19.081331,-155.618917,19.5307')

Range join query:

  select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)

Distance query:

  select * from point where ST_Within(point._c0,'-88.331492,32.324142',10)

Distance join query:

  select * from point p1, point p2 where ST_Within(p1._c0, p2._c0, 10)


Run your code on Apache Spark using "spark-submit"
