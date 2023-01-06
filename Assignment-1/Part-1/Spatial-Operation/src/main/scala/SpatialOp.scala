
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

object SpatialOp {

  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def getMBR(spark: SparkSession, filePath: String): RDD[Double] =
  {
    var polyRdd = new SpatialRDD[Geometry]()
    polyRdd = ShapefileReader.readToGeometryRDD(spark.sparkContext, filePath)

    //TODO
    // transform the coordinate refrenece system of polyRdd from "epsg:2263" to "epsg:4326"
    polyRdd.CRSTransform("epsg:2263","epsg:4236")



    val areaRdd = polyRdd.rawSpatialRDD.rdd.map(f => {
      //TODO
      // return the area of the minimum bounding rectangle or envelop
      f.getEnvelope().getArea;
    })

    areaRdd
  }


}
