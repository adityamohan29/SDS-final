package models

import controllers.service.ManageTrajectory
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import play.api.libs.json._
import play.api.mvc._

import java.io.PrintWriter
import javax.inject._
import scala.collection.JavaConverters._


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
  def addNewDataset(fileName:Option[String]) = Action { implicit request =>
    if (spark == null) {
      sparkInit()
    }
    val content = request.body
    val jsonObject = content.asJson
    new PrintWriter("data/" + fileName.get) { write(Json.toJson(jsonObject).toString()); close }
    Ok
  }

  def getSpatialRangeController(latMin:Option[Double], lonMin:Option[Double], latMax:Option[Double], lonMax:Option[Double], dataset: Option[String]) = Action { implicit request =>
    if(spark == null){
      sparkInit()
    }
    // TODO - Remove below fileName assignment !!!!!!
//    val fileName = "simulated_trajectories"
//    val file_path = "data/" + fileName + ".json"

    val file_path = "data/" + dataset.get

    val df = ManageTrajectory.loadTrajectoryData(spark,file_path)
    val df2 = ManageTrajectory.getSpatialRange(spark,df,latMin.get,lonMin.get ,latMax.get ,lonMax.get)

    val output = df2
      .collect
      .map(
        row => df2
          .columns
          .foldLeft(Map.empty[String, Any])
          (
            (acc, item) => acc + (item -> row.getAs[Any](item))
          )
      )

    implicit val formats: DefaultFormats = DefaultFormats
    val json = Serialization.write(output)

    Ok(json).as("application/json")
  }


  def getKNNTrajectoryController(trajectoryId:Option[Long], k:Option[Int], dataset: Option[String]) = Action { implicit request =>
    if (spark == null) {
      sparkInit()
    }

    // TODO - Remove below fileName assignment !!!!!!
//    val fileName = "simulated_trajectories"
//    val file_path = "data/" + fileName + ".json"

    val file_path = "data/" + dataset.get

    val df = ManageTrajectory.loadTrajectoryData(spark, file_path)
    print(" df1 ", df)



    print("\n============KNN DF================\n")
    val df2 = ManageTrajectory.getKNNTrajectory(spark, df, trajectoryId.get, k.get)
    df2.show(false)
    val output = df2
      .collect
      .map(
        row => df2
          .columns
          .foldLeft(Map.empty[String, Any])
          (
            (acc, item) => acc + (item -> row.getAs[Any](item))
          )
      )

    implicit val formats: DefaultFormats = DefaultFormats
    val json = Serialization.write(output)

    Ok(json).as("application/json")
  }

  def getSpatioTemporalRangeController(timeMin: Option[Long], timeMax: Option[Long], latMin: Option[Double], lonMin: Option[Double], latMax: Option[Double], lonMax: Option[Double], dataset: Option[String]) = Action { implicit request =>
    if (spark == null) {
      sparkInit()
    }

    // TODO - Remove below fileName assignment !!!!!!
//    val fileName = "simulated_trajectories"
//    val file_path = "data/" + fileName + ".json"

    val file_path = "data/" + dataset.get

    val df = ManageTrajectory.loadTrajectoryData(spark, file_path)
    print(" df1 ", df)
    val df2 = ManageTrajectory.getSpatioTemporalRange(spark, df, timeMin.get, timeMax.get, latMin.get, lonMin.get, latMax.get, lonMax.get)


    val output = df2
      .collect
      .map(
        row => df2
          .columns
          .foldLeft(Map.empty[String, Any])
          (
            (acc, item) => acc + (item -> row.getAs[Any](item))
          )
      )

    implicit val formats: DefaultFormats = DefaultFormats
    val json = Serialization.write(output)

    Ok(json).as("application/json")
  }


}