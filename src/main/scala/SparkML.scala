import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Created by Andrii_Krasnolob on 3/31/2016.
  */
object SparkML {
  val inputDataDir: String = "D:\\Share\\Spark_Basics_p3\\Spark_Basics_p3_data\\session.dataset\\"
  val objectsFileName:String = "Objects.csv"
  val targetFileName:String = "Target.csv"
  val dataFileName:String = "all_data.csv"



  def main(args:Array[String]): Unit = {

    if ( System.getProperty("os.name") == "Windows 7") {
      System.setProperty("hadoop.home.dir", "C:\\BigData\\Hadoop")
    }
    val conf = new SparkConf().setAppName("SparkML").setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.textFile(inputDataDir+dataFileName)

    val parsedData = data.map { line =>
      val parts = line.split(',')
            LabeledPoint(parts(0).toDouble, Vectors.dense(parts.tail.map(x => x.toDouble))
      )
    }
    val splits = parsedData.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))


    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 3 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 32

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification forest model:\n" + model.toDebugString)

    // Save and load model
    model.save(sc, "target/tmp/myRandomForestClassificationModel")
    val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel")
  }
}
