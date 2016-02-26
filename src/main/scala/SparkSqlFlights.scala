import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql._

/**
  * Created by Andrii_Krasnolob on 2/26/2016.
  */
object SparkSqlFlights {
  case class Carrier(code: String, description: String)
  case class Airport(iata:String, airport:String, city:String, state:String, country:String, lat:Double, long:Double)
  case class Flight(year:Int/*0*/, month:Int/*1*/, day:Int/*2*/,carrierCode:String/*8*/, origin:String/*16*/, dest:String/*17*/)
def main(args:Array[String]){
  System.setProperty("hadoop.home.dir", "C:\\")

  val conf = new SparkConf().setAppName("SparkSqlFlights").setMaster("local")
  val sc = new SparkContext(conf)
  val sql = new SQLContext(sc)
  import sql.implicits._

  // Define the schema using a case class.


  // Create an RDD of Person objects and...
 // val people = sc.textFile()
  sc.textFile("D:\\Share\\Hive_Basics_p1\\airports.carrier.dataset\\carriers.csv")
    .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    .map(_.split(","))
    .map(p => Carrier(p(0), p(1)))
    .toDF.registerTempTable("carriers")


  sc.textFile("D:\\Share\\Hive_Basics_p1\\airports.carrier.dataset\\airports.csv")
    .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    .map(_.split(","))
    .map(p => Airport(p(0), p(1), p(2), p(3), p(4), p(5).toDouble, p(6).toDouble))
    .toDF.registerTempTable("airports")

  sc.textFile("D:\\Share\\Hive_Basics_p1\\airports.carrier.dataset\\2007.csv")
    .cache()
    .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    .map(_.split(","))
    .map(p => Flight(p(0).toInt, p(1).toInt, p(2).toInt, p(8), p(16), p(17)))
    .toDF.registerTempTable("flights")

//  sql.sql("select f.carrierCode, count(*) from flights f join carriers c on c.code=f.carrierCode group by f.carrierCode")
  sql.sql("select distinct  carrierCode from flights")
    .collect()
    .foreach(println)

}
}
