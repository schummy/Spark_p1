import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.orc._
/**
  * Created by Andrii_Krasnolob on 2/26/2016.
  */
object SparkSqlFlights {
  case class Carrier(code: String, description: String)
  case class Airport(iata:String, airport:String, city:String, state:String, country:String, lat:Double, long:Double)
  case class Flight(year:Int/*0*/, month:Int/*1*/, day:Int/*2*/,carrierCode:String/*8*/, origin:String/*16*/, dest:String/*17*/, airports:Array[String])
  //val dataDir = "/Users/user/bigData/EPAM_hadoop_training/Hive_Basics_p1/airports.carrier.dataset/"
  val dataDir = "file://D:\\Share\\Hive_Basics_p1\\airports.carrier.dataset\\"
  def main(args:Array[String]){
    System.setProperty("hadoop.home.dir", "C:\\BigData\\Hadoop")

  val conf = new SparkConf().setAppName("SparkSqlFlights").setMaster("local")
    conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
  val sc = new SparkContext(conf)
  //val sql = new SQLContext(sc)
  val hql = new HiveContext(sc)
  import hql.implicits._

  // Define the schema using a case class.


  val carrierRegExp = "\"(.*)\",\"(.*)\""r

  sc.textFile(dataDir + "carriers.csv")
    .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    .map(line => { var carrierRegExp(code, description) = line
      new Carrier(code, description) })
   // .toDF.registerTempTable("carriers")


  val airportsRegExp = "\"(.*)\",\"(.*)\",\"?(.*[^\"])\"?,\"?(.*[^\"])\"?,\"(.*[^\"])\",([0-9.-]+),([0-9.-]+)"r

  sc.textFile(dataDir + "airports.csv")
    .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    .map(line => { var airportsRegExp(iata,airport,city,state,country,lat,long) = line
      new Airport(iata,airport,city,state,country,lat.toDouble,long.toDouble) })
   // .toDF.registerTempTable("airports")

  sc.textFile(dataDir + "2007.csv")
    .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    .map(_.split(","))
    .map(p => Flight(
      p(0).toInt,
      p(1).toInt,
      p(2).toInt,
      p(8),
      p(16),
      p(17),
     Array(p(16), p(17)))
    )
    .toDF.registerTempTable("flights")
   /* sql.cacheTable("flights")
    sql.cacheTable("airports")*/

/* task 6
    sql.sql("select c.description, f.numberOfFlights " +
      "from (select carrierCode, count(*) as numberOfFlights from flights group by carrierCode ) f " +
      "join carriers c on c.code=f.carrierCode " +
      "order by numberOfFlights desc")
      .collect()
     .foreach(println)
*/



/* task 7
    sql.sql("select iata from airports where city=\"New York\"")
      .registerTempTable("NYC_airports")
    sql.sql("select sum(number) from (select count(*) number from flights f join NYC_airports a on a.iata=f.origin " +
      "where year=2007 and month=6 " +
      "union " +
      "select count(*) number from flights f join NYC_airports a on a.iata=f.dest " +
      "where year=2007 and month=6 ) tmp")
      .collect()
      .foreach(println)
*/

// task 8
/*    sql.sql("select distinct iata, airport from airports where country=\"USA\"")
      .registerTempTable("USA_airports")*/

/*    sql.sql(
  "select dest as iata, count(*) number from flights  join USA_airports a on dest=a.iata " +
  "where month between 6 and 8 " +
  "group by dest " +
    "order by number desc " +
    "limit 5"
)
  .collect()
  .foreach(println)*/

/*    sql.sql(
    "select airport from " +
    "( select a.iata, count(*) flightsCount from (" +
        "select dest as iata from flights " +
        "where month between 6 and 8 " +
        "union all " +
        "select origin as iata from flights " +
        "where month between 6 and 8 " +
    ") tmp join USA_airports a on tmp.iata=a.iata " +
    "group by a.iata " +
    "order by flightsCount desc " +
    "limit 5 )top5 join airports a on top5.iata=a.iata"
)*/
      hql.sql("SELECT year, month, day, airport FROM flights LATERAL VIEW explode(airports) tmpTable AS airport limit 100")
  .collect()
  .foreach(println)

/*    sql.sql(
  "select tmp.iata, sum(number) flightsCount from (" +
  "select dest as iata, count(*) number from flights " +
  "where month between 6 and 8 " +
  "group by dest " +
  "union " +
  "select origin as iata, count(*) number from flights " +
  "where month between 6 and 8 " +
  "group by origin) tmp join USA_airports a on tmp.iata=a.iata " +
    "group by tmp.iata " +
    "order by flightsCount desc " +
    "limit 5"
)
  .collect()
  .foreach(println)*/



/* task 9
  sql.sql("select description from (select carrierCode, count(*) numberOfLights from flights group by carrierCode order by numberOfLights limit 1) a " +
    "join carriers on carrierCode=code")
  .collect()
  .foreach(println)
*/
}
}
