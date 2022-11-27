package controllers.service

import org.apache.spark.sql.functions.{array, collect_list}
import org.apache.spark.sql.{DataFrame, SparkSession}
object ManageTrajectory {

//  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
//  Logger.getLogger("org.apache").setLevel(Level.WARN)
//  Logger.getLogger("akka").setLevel(Level.WARN)
//  Logger.getLogger("com").setLevel(Level.WARN)


  def loadTrajectoryData(spark: SparkSession,filePath: String): DataFrame =
  {
    var trajectoryDf = spark.read.option("multiline","true").format("json").load(path = filePath)

    trajectoryDf = trajectoryDf.select(
      org.apache.spark.sql.functions.explode(trajectoryDf.col("trajectory")).as("trajectory"),
      trajectoryDf.col("trajectory_id").as("trajectory_id"),
      trajectoryDf.col("vehicle_id").as("vehicle_id"))

    var flattenedTrajectoryDf = trajectoryDf.select(trajectoryDf.col("trajectory_id"),
      trajectoryDf.col("vehicle_id").as("vehicle_id"),
      trajectoryDf.col("trajectory.location")(0).as("latitude"),
      trajectoryDf.col("trajectory.location")(1).as("longitude"),
      trajectoryDf.col("trajectory.timestamp"))

    flattenedTrajectoryDf.createOrReplaceTempView("inputtable")
    var spatialDf = spark.sql(
      s"""
         |SELECT trajectory_id, vehicle_id, timestamp, ST_Point(CAST(inputtable.longitude AS Decimal(24,20)),CAST(inputtable.latitude AS Decimal(24,20))) AS point
         |FROM inputtable
      """.stripMargin)

    spatialDf
  }


  def getSpatialRange(spark: SparkSession, dfTrajectory: DataFrame, latMin: Double, lonMin: Double, latMax: Double, lonMax: Double): DataFrame =
  {

    dfTrajectory.createOrReplaceTempView("inputtable")
    var query =
      s"""
         |SELECT trajectory_id, vehicle_id, timestamp, point, ST_ASTEXT(point) as p1, ST_Y(point) as y1,ST_X(point) as x1
         |FROM inputtable
         |WHERE ST_Contains(ST_PolygonFromEnvelope($lonMin,$latMin,$lonMax,$latMax), point)
        """
    var spatialDf = spark.sql(query.stripMargin)
    spatialDf.show(truncate = false)

    spatialDf = spatialDf.withColumn("location", array(spatialDf("y1"), spatialDf("x1")))
    //spatialDf.show()
    var collected_spatialDf = spatialDf.groupBy(spatialDf.col("trajectory_id"),spatialDf.col("vehicle_id")).agg(collect_list(spatialDf.col("timestamp")).as("timestamp"),collect_list(spatialDf.col("location")).as("location"))
    collected_spatialDf.show()
    collected_spatialDf
  }


  def getSpatioTemporalRange(spark: SparkSession, dfTrajectory: DataFrame, timeMin: Long, timeMax: Long, latMin: Double, lonMin: Double, latMax: Double, lonMax: Double): DataFrame =
  {
    dfTrajectory.createOrReplaceTempView("inputtable")
    var query =
      s"""
         |SELECT trajectory_id, vehicle_id, timestamp, point, ST_Y(point) as y1,ST_X(point) as x1
         |FROM inputtable
         |WHERE ST_Contains(ST_PolygonFromEnvelope($lonMin,$latMin,$lonMax,$latMax), point)
         |AND timestamp >= $timeMin
         |AND timestamp <= $timeMax
        """
    var spatioTemporalDf = spark.sql(query.stripMargin)
    spatioTemporalDf.show(truncate = false)
    spatioTemporalDf = spatioTemporalDf.withColumn("location", array(spatioTemporalDf("y1"), spatioTemporalDf("x1")))
    //spatialDf.show()
    spatioTemporalDf  = spatioTemporalDf.groupBy(spatioTemporalDf.col("trajectory_id"), spatioTemporalDf.col("vehicle_id")).agg(collect_list(spatioTemporalDf.col("timestamp")).as("timestamp"), collect_list(spatioTemporalDf.col("location")).as("location"))
    spatioTemporalDf .show()

    spatioTemporalDf
  }


  def getKNNTrajectory(spark: SparkSession, dfTrajectory: DataFrame, trajectoryId: Long, neighbors: Int): DataFrame =
  {
    /* TO DO */

    dfTrajectory.createOrReplaceTempView("inputtable")
    var query =
      s"""
         SELECT  i1.trajectory_id from inputtable as i1 cross join inputtable as i2 where i2.trajectory_id = $trajectoryId AND i1.trajectory_id!= $trajectoryId group by i1.trajectory_id order by min(ST_DISTANCE(i1.point,i2.point)) limit $neighbors
        """
    var knnDf = spark.sql(query.stripMargin)
    knnDf.show(truncate = false)

    knnDf
  }


}
