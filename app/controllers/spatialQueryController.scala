package models

import controllers.service.ManageTrajectory
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Row, SparkSession}
import play.api.libs.json._
import play.api.mvc._
import org.apache.spark.sql.functions.{col, to_json}
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSONObject
import scala.collection.JavaConverters._
import javax.inject._
import scala.collection.mutable


/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */

@Singleton
class spatialQueryController @Inject()(val controllerComponents: ControllerComponents)
  extends BaseController {


  implicit val getSpatialRangeDTO = Json.format[getSpatialRangeDTO]
  def getSpatialRangeController() = Action { implicit request =>
    val content = request.body
    val jsonString = content.asJson.get.toString()
    val json: JsValue = Json.parse(jsonString)

    //parsing the json
    val lat1 = (json \ "lat1").as[Double]
    val lon1 = (json \ "lon1").as[Double]
    val lat2 = (json \ "lat2").as[Double]
    val lon2 = (json \ "lon2").as[Double]

    val spark: SparkSession = SparkSession.builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName)
      .master("local[*]")
      .appName("SDSE-Phase-1-Apache-Sedona")
      .getOrCreate()

    SedonaSQLRegistrator.registerAll(spark)
    SedonaVizRegistrator.registerAll(spark)

    val file_path = "/Users/dhanushkamath/Development/ASU_Projects/Spatial_Data_Science/sds-project-phase-1/data/simulated_trajectories.json"
    val df = ManageTrajectory.loadTrajectoryData(spark,file_path)
    print(" df1 ",df)
    val df2 = ManageTrajectory.getSpatialRange(spark,df,lat1,lon1,lat2,lon2)

    print("dataframe, ",df2)
    print(df2.collectAsList().size())
    print("\n============================\n")

    // print(df2.collectAsList())

    def convertRowToJSON(row: Row): String = {
      val m = row.getValuesMap(row.schema.fieldNames)
      JSONObject(m).toString()
    }

    var output = ListBuffer[String]()
    val collected = df2.collectAsList()

    for (k <- collected.asScala){
      output += convertRowToJSON(k)
    }
    print(output.mkString(","))
    //dummy json respons
//    val spatialRangeQuery: Option[getSpatialRangeDTO] =
//      content.asJson.flatMap(
//        Json.fromJson[getSpatialRangeDTO](_).asOpt
//      )
//    Created(Json.toJson(spatialRangeQuery))
    Created(output.mkString(","))
  }



}