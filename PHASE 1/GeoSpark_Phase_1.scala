/* 1. Create GeoSpark SpatialRDD (PointRDD) */
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.FileDataSplitter;

val pointRDD = new PointRDD(sc, "hdfs://master:54310/arealm.csv", 0, FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY); 



/* 2. Spatial Range Query: Query the PointRDD using this query window */

/* 2a. Query the PointRDD using this window */
import org.datasyslab.geospark.spatialOperator.RangeQuery; 
import com.vividsolutions.jts.geom.Envelope;

val pointRDD = new PointRDD(sc, "hdfs://master:54310/arealm.csv", 0, FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY); 
val queryEnvelope = new Envelope (-113.79,-109.73,32.99,35.08);
val resultSize = RangeQuery.SpatialRangeQuery(pointRDD, queryEnvelope, false, false).count();


/* 2b. Build R-Tree index on PointRDD then query this PointRDD */
import org.datasyslab.geospark.enums.IndexType;

val pointRDD = new PointRDD(sc, "hdfs://master:54310/arealm.csv", 0, FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY); 
val queryEnvelope = new Envelope (-113.79,-109.73,32.99,35.08);
pointRDD.buildIndex(IndexType.RTREE,false);
val resultSize = RangeQuery.SpatialRangeQuery(pointRDD, queryEnvelope, false, true).count();



/* 3. Spatial KNN query: Query the PointRDD using this query point */
/* 3a. Query the PointRDD and find 5 Nearest Neighbors*/
import org.datasyslab.geospark.spatialOperator.KNNQuery;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Coordinate;

val pointRDD = new PointRDD(sc, "hdfs://master:54310/arealm.csv", 0, FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY); 
val fact=new GeometryFactory();
val queryPoint=fact.createPoint(new Coordinate(-109.73, 35.08));
val resultSize = KNNQuery.SpatialKnnQuery(pointRDD, queryPoint, 5,false);


/* 3b. Build R-Tree index on PointRDD then query this PointRDD again */
val pointRDD = new PointRDD(sc, "hdfs://master:54310/arealm.csv", 0, FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY); 
val fact=new GeometryFactory();
val queryPoint=fact.createPoint(new Coordinate(-109.73, 35.08));
pointRDD.buildIndex(IndexType.RTREE,false);
val resultSize = KNNQuery.SpatialKnnQuery(pointRDD, queryPoint, 5,true);


/* 4. Spatial Join query: Create a GeoSpark RectangleRDD and use it to join PointRDD */

import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;
import org.datasyslab.geospark.enums.GridType;

/* 4a. Join the PointRDD using Equal grid without R-Tree index*/

val pointRDD = new PointRDD(sc, "hdfs://master:54310/arealm.csv", 0, FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY); 
val rectangleRDD = new RectangleRDD(sc, "hdfs://master:54310/zcta510.csv", 0, FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY); 
pointRDD.spatialPartitioning(GridType.EQUALGRID);
rectangleRDD.spatialPartitioning(pointRDD.grids);
val resultSize = JoinQuery.SpatialJoinQuery(pointRDD,rectangleRDD,false,false).count();

/* 4b. Join the PointRDD using Equal grid with R-Tree index */
val pointRDD = new PointRDD(sc, "hdfs://master:54310/arealm.csv", 0, FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY); 
val rectangleRDD = new RectangleRDD(sc, "hdfs://master:54310/zcta510.csv", 0, FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY); 
pointRDD.spatialPartitioning(GridType.EQUALGRID);
pointRDD.buildIndex(IndexType.RTREE,true);
rectangleRDD.spatialPartitioning(pointRDD.grids);
val resultSize = JoinQuery.SpatialJoinQuery(pointRDD,rectangleRDD,true, false).count();

/* 4c. Join the PointRDD using R-Tree grid without R-Tree index */
val pointRDD = new PointRDD(sc, "hdfs://master:54310/arealm.csv", 0, FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY); 
val rectangleRDD = new RectangleRDD(sc, "hdfs://master:54310/zcta510.csv", 0, FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY); 
pointRDD.spatialPartitioning(GridType.RTREE);
pointRDD.buildIndex(IndexType.RTREE,false);
rectangleRDD.spatialPartitioning(pointRDD.grids);
val resultSize = JoinQuery.SpatialJoinQuery(pointRDD,rectangleRDD,false, true).count();
