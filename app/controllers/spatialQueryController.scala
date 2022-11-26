package models

import controllers.service.ManageTrajectory
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Row, SparkSession}
import play.api.libs.json._
import play.api.mvc._

import javax.inject._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSONObject


/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */

@Singleton
class spatialQueryController @Inject()(val controllerComponents: ControllerComponents)
  extends BaseController {

  var spark: SparkSession = null

  def sparkInit(): Unit ={
    spark = SparkSession.builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName)
      .master("local[*]")
      .appName("SDSE-Phase-1-Apache-Sedona")
      .getOrCreate()

    SedonaSQLRegistrator.registerAll(spark)
    SedonaVizRegistrator.registerAll(spark)
  }


  implicit val getSpatialRangeDTO = Json.format[getSpatialRangeDTO]
  def getSpatialRangeController(latMin:Option[Double], lonMin:Option[Double], latMax:Option[Double], lonMax:Option[Double]) = Action { implicit request =>
    if(spark == null){
      sparkInit()
    }

    val file_path = "/Users/dhanushkamath/Development/ASU_Projects/Spatial_Data_Science/sds-project-phase-1/data/simulated_trajectories.json"
    val df = ManageTrajectory.loadTrajectoryData(spark,file_path)
    print(" df1 ",df)
    val df2 = ManageTrajectory.getSpatialRange(spark,df,latMin.get,lonMin.get ,latMax.get ,lonMax.get)


    // print("\n============FLAT DF================\n")
    val concatArrayTimestamps = udf((value: Seq[Long]) => {
      value.mkString("[", ",", "]")
    })

    val concatArrayLocation = udf((value: Seq[Seq[Long]]) => {
      value.map(_.mkString("[", ",", "]")).mkString("[", ",", "]")
    })

    val flat_df = df2.withColumn("location", concatArrayLocation(df2.col("location"))).withColumn("timestamp", concatArrayTimestamps(df2.col("timestamp")))

    def convertRowToJSON(row: Row): String = {
      val m = row.getValuesMap(row.schema.fieldNames)
      JSONObject(m).toString()
    }

    var output = ListBuffer[String]()
    val collected = flat_df.collectAsList()

    for (k <- collected.asScala){
      output += convertRowToJSON(k)
    }
    print(output.mkString(","))

    Ok(Json.toJson(Json.parse("{\"output\":["+output.mkString(",")+"]}")))
  }


  def getSpatioTemporalRangeController(timeMin:Option[Long],timeMax:Option[Long],latMin:Option[Double], lonMin:Option[Double], latMax:Option[Double], lonMax:Option[Double]) = Action { implicit request =>
    if (spark == null) {
      sparkInit()
    }

    val file_path = "/Users/dhanushkamath/Development/ASU_Projects/Spatial_Data_Science/sds-project-phase-1/data/simulated_trajectories.json"
    val df = ManageTrajectory.loadTrajectoryData(spark, file_path)
    print(" df1 ", df)
    val df2 = ManageTrajectory.getSpatioTemporalRange(spark, df, timeMin.get, timeMax.get, latMin.get, lonMin.get, latMax.get, lonMax.get)


    // print("\n============FLAT DF================\n")
    val concatArrayTimestamps = udf((value: Seq[Long]) => {
      value.mkString("[", ",", "]")
    })

    val concatArrayLocation = udf((value: Seq[Seq[Long]]) => {
      value.map(_.mkString("[", ",", "]")).mkString("[", ",", "]")
    })

    val flat_df = df2.withColumn("location", concatArrayLocation(df2.col("location"))).withColumn("timestamp", concatArrayTimestamps(df2.col("timestamp")))

    def convertRowToJSON(row: Row): String = {
      val m = row.getValuesMap(row.schema.fieldNames)
      JSONObject(m).toString()
    }

    var output = ListBuffer[String]()
    val collected = flat_df.collectAsList()

    for (k <- collected.asScala) {
      output += convertRowToJSON(k)
    }
    print(output.mkString(","))
    //dummy json respons
//        val spatialRangeQuery: Option[getSpatialRangeDTO] =
//          content.asJson.flatMap(
//            Json.fromJson[getSpatialRangeDTO](_).asOpt
//          )
//        Created(Json.toJson(spatialRangeQuery))
    Ok(Json.toJson(Json.parse("{\"output\":[" + output.mkString(",") + "]}")))
  }


}