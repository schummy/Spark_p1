import org.apache.log4j.{Level, LogManager}
import org.apache.spark._
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.ReceiverInputDStream

/**
  * Created by Andrii_Krasnolob on 3/3/2016.
  */
object SparkStreamSandbox {
  val networkDeviceName: String = "\\Device\\NPF_{6AE37950-B19A-4A4F-B3AC-2425236BE527}"
  val checkpointDirectory: String = "D:\\Share\\Spark_Basics_p2\\checkpoint"
  val numberOfThreads = 3
  val batchInterval: Long = 1

  def main(args:Array[String]) {
    val setTmp = new IPSettings()
    setTmp.add("255.255.255.255", 1, 10.0, 500L)
    setTmp.add("255.255.255.255", 2, 20.0, 500L)
    setTmp.add(null, 2, 30.0, 500L)
    setTmp.add(null, 1, 40.0, 400L)
    println(setTmp)

    println (setTmp.get("255.255.285.255"))

    if ( System.getProperty("os.name") == "Windows 7") {
      System.setProperty("hadoop.home.dir", "C:\\BigData\\Hadoop")
    }
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkpointDirectory, createContext _)
    LogManager.getRootLogger.setLevel(Level.ERROR)
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }

  def processData(ssc: StreamingContext): Unit = {
    val lines = ssc.receiverStream(new CustomReceiverJnetPcap(networkDeviceName))


    val settings = loadSettings(ssc.sparkContext).collect

    val ipSettings = new IPSettings
    settings.foreach(r => ipSettings.add(
        r.getAs[String]("hostIp")
      , r.getAs[Long]("limitType").toInt
      , r.getAs[Double]("value")
      , r.getAs[Long]("period")))

    val pairs = lines
        .reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2))
      .map(r =>(r._1, (new IPPacketsInfo(r._2._1, r._2._2), ipSettings.get(r._1) ) ) )

    //pairs.foreachRDD(x=> x.foreach(a => println(a._2.toString)))

    val stateSpec = StateSpec.function(trackStateFunc _)
    val wordCountStateStream = pairs.mapWithState(stateSpec)
    //wordCountStateStream.print()

    val stateSnapshotStream = wordCountStateStream.stateSnapshots()

    stateSnapshotStream.foreachRDD(rdd => {
      println("?"*40)
      rdd.sortByKey().foreach(a => println(a._1 + " => " + a._2.toString))
    })
  }



  def createContext(): StreamingContext = {
    //println("-"*20+"createContext"+"-"*20)
    val conf = new SparkConf().setMaster(s"local[$numberOfThreads]").setAppName(SparkStreamSandbox.getClass.toString)
    val ssc = new StreamingContext(conf, Seconds(batchInterval))
    ssc.checkpoint(checkpointDirectory)
    processData(ssc)
    ssc.remember(Seconds(10))
    ssc
  }

  def loadSettings(sc: SparkContext): DataFrame = {
    val hql = new HiveContext(sc)
    populateTestingData(hql)
    val settings: DataFrame = hql.sql("Select " +
      "s1.hostIp, " +
      "s1.limitType,  " +
      "nvl(s1.value, s2.value) as value, " +
      "nvl(s1.period, s2.period) as period " +
      "from settings s1 left join (select * from settings where hostIp is null) s2 on s1.limitType = s2.limitType").toDF()
    settings.show()
    validateSettings(settings)
    settings
  }

  def populateTestingData(hql: HiveContext): Unit = {
    hql.read.json("D:\\Share\\Spark_Basics_p2\\hive_settings.json").registerTempTable("settings")
  }

  def validateSettings(settings: DataFrame): Unit = {
    val defaultRecord = settings.filter("hostIp is null")
   // defaultRecord.show()
    if (defaultRecord.count() != 2) {
      LogManager.getRootLogger.error("Table should always has two DEFAULT parameters")
      throw new scala.RuntimeException("Table should always has two DEFAULT parameters")
    }
    val tmpDF = defaultRecord.filter("value is not null and period is not null and (limitType = 1  or limitType = 2)")
    if (tmpDF.count() != 2) {
      LogManager.getRootLogger.error("Invalid default settings")
      throw new scala.RuntimeException("Invalid default settings")
    }
  }

  def trackStateFunc(batchTime: Time, key: String, value: Option[(IPPacketsInfo, IPLimits)], state: State[IPState]): Option[(String, IPState)] = {
  //def trackStateFunc(batchTime: Time, key: String, value: Option[IPPacketsInfo], state: State[IPState]): Option[(String, IPState)] = {
/*    val emptyValue = new IPPacketsInfo(0,0)
    var currentState = state.getOption().getOrElse(emptyValue)
    val sum = value.getOrElse(emptyValue).wirelen + currentState.wirelen;
    val count =  value.getOrElse(emptyValue).count + currentState.count;
    currentState.setWireLen(sum)
    currentState.setCount(count)
    currentState.setTimeStamp(batchTime)
    val output = (key, currentState)
    state.update(currentState)
    Some(output)*/

    val emptyValue = (new IPPacketsInfo(0,0), null)
    //val emptyValue = new IPPacketsInfo(0,0)
    val emptyState = new IPState()
    //val currentSettings2:IPSettings = new IPSettings()

    var currentState = state.getOption().getOrElse(emptyState)
    var (currentValue, currentSettings) = value.getOrElse(emptyValue)
    currentValue.setTimeStamp(batchTime)
    println("-"*30 + key + "-"*3 + currentValue.count + "-"*30)


    currentState.addValue(currentValue, currentSettings )

    val output = (key, currentState)
    state.update(currentState)
    Some(output)
  }
}
