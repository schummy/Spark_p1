import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkContext, SparkConf}

/**
  * Created by Andrii_Krasnolob on 2/26/2016.
  */

object LogParser {
  def toIntOrZero(s: String): Int = {
    try {
      s.toInt
    } catch {
      case e: NumberFormatException => 0
    }
  }
  var accumIE:Accumulator[Int]=null
  var accumMozilla:Accumulator[Int]=null
  var accumOther:Accumulator[Int]=null
  val patternIE = ".*(MSIE).*".r
  val poattermMozilla = ".*(Firefox).*".r




  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\")

    val conf = new SparkConf().setAppName("LogParser").setMaster("local")
    val sc = new SparkContext(conf)
    initAccumulators(sc)

    val textFile = sc.textFile("D:\\Share\\Spark_Basics_p1\\web.server.access.log\\access_logs\\input\\000000")
    //val textFile = sc.textFile("D:\\Share\\Spark_Basics_p1\\web.server.access.log\\access_logs\\input\\small")

    var output: RDD[String] = mapReduce(textFile)

    val file = "./output"
    output.saveAsTextFile(file)
    //println(output)

    println(s"accumIE=$accumIE, accumMozilla=$accumMozilla, accumOther=$accumOther")
    // Thread.sleep(100000)
  }

  def initAccumulators(sc: SparkContext): Unit = {
    println("-"*20+"initAccumulators call"+"-"*20)
    accumIE = sc.accumulator(0, "IE Accumulator")
    accumMozilla = sc.accumulator(0, "Mozilla Accumulator")
    accumOther = sc.accumulator(0, "Other Accumulator")
  }

  def mapReduce(textFile: RDD[String]): RDD[String] = {
    textFile
      .map(parseLine _)
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      //.repartition(1)
      .map(key_value => Array(key_value._1, key_value._2._1, key_value._2._1 / key_value._2._2).mkString(","))
    //.toDebugString
  }

  def updateAccumulator(agent: String): Unit = {

    agent match {
      case patternIE(a) => accumIE += 1
      case poattermMozilla(a) => accumMozilla += 1
      case _ => accumOther += 1
    }
  }

  def parseLine(s: String): (String, (Int, Double)) = {
    val regexpPattern = "^(ip[\\d]+) \\S+ \\S+ \\[[\\w:/]+\\s[+\\-]\\d{4}\\] \".+?\" \\d{3} (\\S+) \"[^\"]*\" \"([^\"]+)\"" r
    val regexpPattern(ip, size, agent) = s
    updateAccumulator(agent)
    (ip, (toIntOrZero(size), 1.0))
  }


}