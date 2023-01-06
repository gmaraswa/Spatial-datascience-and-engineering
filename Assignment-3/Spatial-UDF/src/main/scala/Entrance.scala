import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator
import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.sedona_sql.expressions.implicits.GeometryEnhancer
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, Point, Polygon}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.locationtech.jts.io.WKBReader

import java.io.File
import scala.collection.mutable
import scala.reflect.io.Directory


object Entrance extends App {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def ST_GeomometryFromGeoHash(geoHash: String): Geometry = {
    deserialize(decodeGeoHashBBox(geoHash).getBbox().toPolygon().toGenericArrayData)
  }

  private def decodeGeoHashBBox(geohash: String): LatLon = {
    val bits = Seq(16, 8, 4, 2, 1)
    val base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
    val latLon = LatLon(mutable.Seq(-180, 180), mutable.Seq(-90, 90))
    val geoHashLowered = geohash.toLowerCase()
    val geoHashLength = geohash.length
    var isEven = true
    for (i <- Range(0, geoHashLength)) {
      val c = geoHashLowered(i)
      val cd = base32.indexOf(c).toByte
      for (j <- Range(0, 5)) {
        val mask = bits(j).toByte
        val index = if ((mask & cd) == 0) 1 else 0
        if (isEven) {
          latLon.lons(index) = (latLon.lons.head + latLon.lons(1)) / 2
        }
        else {
          latLon.lats(index) = (latLon.lats.head + latLon.lats(1)) / 2
        }
        isEven = !isEven
      }
    }
    latLon
  }

  def deserialize(values: ArrayData): Geometry = {
    val reader = new WKBReader()
    reader.read(values.toByteArray())
  }

  override def main(args: Array[String]) {

    var spark: SparkSession = SparkSession.builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName)
      .master("local[*]")
      .appName("Spatial-UDF-Apache-Sedona")
      .getOrCreate()

    SedonaSQLRegistrator.registerAll(spark)
    SedonaVizRegistrator.registerAll(spark)
    spark.udf.register("ST_GeomFromGeoHash", ST_GeomometryFromGeoHash _)
    computeGeoStringAndSaveToFile(spark, args(1), args(0))
  }


  def computeGeoStringAndSaveToFile(spark: SparkSession, input_path: String, output_path: String): Unit = {
    val dir = new Directory(new File(output_path))
    dir.deleteRecursively()
    val df = spark.read.option("header", true)
      .csv(input_path)
    df.createOrReplaceTempView("geoHashView")
    val sqlDF = spark.sql("Select community,ST_AsText(ST_GeomFromGeoHash(geohash)) as column from geoHashView order ")
    sqlDF.sort(asc("community")).coalesce(1).write.csv(output_path)
  }
}

private case class LatLon(var lons: mutable.Seq[Double], var lats: mutable.Seq[Double]) {
  def getBbox(): BBox = BBox(
    startLat = lats.head,
    endLat = lats(1),
    startLon = lons.head,
    endLon = lons(1)
  )
}
private case class BBox(startLon: Double, endLon: Double, startLat: Double, endLat: Double){
  private val geometryFactory = new GeometryFactory()
  def toPolygon(): Polygon = {
    geometryFactory.createPolygon(
      Array(
        new Coordinate(startLon, startLat),
        new Coordinate(startLon, endLat),
        new Coordinate(endLon, endLat),
        new Coordinate(endLon, startLat),
        new Coordinate(startLon, startLat)
      )
    )
  }
}