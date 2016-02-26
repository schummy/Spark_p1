import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

/**
  * Created by Andrii_Krasnolob on 2/26/2016.
  */
class SparkTestSpec extends FlatSpec with Matchers with BeforeAndAfterAll  {
  protected var sc: SparkContext = null

  override protected def beforeAll(): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\")

    val conf = new SparkConf().setAppName("LogParser").setMaster("local")
    sc = new SparkContext(conf)
    LogParser.initAccumulators(sc)
  }

  override protected def afterAll(): Unit = {
    sc.stop()
  }
}
