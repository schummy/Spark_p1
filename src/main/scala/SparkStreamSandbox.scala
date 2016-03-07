import org.apache.log4j.{Level, LogManager}
import org.apache.spark._
import org.apache.spark.sql.DataFrame
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

    if ( System.getProperty("os.name") == "Windows 7") {
      System.setProperty("hadoop.home.dir", "C:\\BigData\\Hadoop")
    }
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkpointDirectory, createContext _)
    LogManager.getRootLogger.setLevel(Level.ERROR)
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }

  def processData(ssc: StreamingContext, settingsDF:DataFrame): Unit = {
    val lines = ssc.receiverStream(new CustomReceiverJnetPcap(networkDeviceName))
    val pairs = lines.map(r =>(r._1, new IPPacketsInfo(r._2, 1)))
    //pairs.foreachRDD(x=> x.foreach(a => println(a._2.toString)))

    val stateSpec = StateSpec.function(trackStateFunc _)
    val wordCountStateStream = pairs.mapWithState(stateSpec)
    //wordCountStateStream.print()

    val stateSnapshotStream = wordCountStateStream.stateSnapshots()

    stateSnapshotStream.foreachRDD(rdd => {
      println("-"*40)
      rdd.sortByKey().foreach(a => println(a._1 + " => " + a._2.toString))
    })
  }

  def createContext(): StreamingContext = {
    //println("-"*20+"createContext"+"-"*20)
    val conf = new SparkConf().setMaster(s"local[$numberOfThreads]").setAppName(SparkStreamSandbox.getClass.toString)
    val ssc = new StreamingContext(conf, Seconds(batchInterval))
    val settings: DataFrame = getSettings(ssc)
    ssc.checkpoint(checkpointDirectory)
    processData(ssc, settings)
    ssc.remember(Minutes(1))
    ssc
  }

  def getSettings(ssc: StreamingContext): DataFrame = {
    val hql = new HiveContext(ssc.sparkContext)
    populateTestingData(hql)
    val settings: DataFrame = hql.sql("Select * from settings").toDF()
    validateSettings(settings)
    settings
  }

  def populateTestingData(hql: HiveContext): Unit = {
    hql.read.json("D:\\Share\\Spark_Basics_p2\\hive_settings.json").registerTempTable("settings")
  }

  def validateSettings(settings: DataFrame): Unit = {
    val defaultRecord = settings.filter("hostIp is null")
    //defaultRecord.show()
    if (defaultRecord.count() == 0) {
      LogManager.getRootLogger.error("No default settings found")
      throw new scala.RuntimeException("No default settings found")
    }
    val tmpDF = defaultRecord.filter("value is not null and period is not null and (limitType = 1  or limitType = 2)")
    if (tmpDF.count() == 0) {
      LogManager.getRootLogger.error("Invalid default settings found")
      throw new scala.RuntimeException("Invalid default settings found")
    }
  }

  def trackStateFunc(batchTime: Time, key: String, value: Option[IPPacketsInfo], state: State[IPState]): Option[(String, IPState)] = {
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

    val emptyValue = new IPPacketsInfo(0,0)
    val emptyState = new IPState()
    val settings:Settings = new Settings("255.255.255.255", 1, 10.0, 500L)
    var currentState = state.getOption().getOrElse(emptyState)
    var currentValue:IPPacketsInfo = value.getOrElse(emptyValue)

    currentState.addValue(currentValue, settings)
    val output = (key, currentState)
    state.update(currentState)
    Some(output)
  }
}
