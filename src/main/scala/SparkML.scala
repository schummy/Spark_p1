import java.lang.Math

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.optimization.L1Updater
/**
  * Created by Andrii_Krasnolob on 3/31/2016.
  */
object SparkML {
  var inputDataDir: String = ""
  val objectsFileName:String = "Objects.csv"
  val targetFileName:String = "Target.csv"
  val dataFileName:String = "all_data.csv"
  val libsvmDataFileName = "sample_libsvm_data.txt"
  val sampleDataFileName = "sample_tree_data.csv"



  def main(args:Array[String]): Unit = {

    if ( System.getProperty("os.name") == "Windows 7") {
      System.setProperty("hadoop.home.dir", "C:\\BigData\\Hadoop")
      inputDataDir = "D:\\Share\\Spark_Basics_p3\\Spark_Basics_p3_data\\session.dataset\\"

    } else {
      inputDataDir = "/Users/user/bigData/EPAM_hadoop_training/Spark_Basics_p3_data/session.dataset/"
    }
    val conf = new SparkConf()
      .setAppName("SparkML")
      .setMaster("local")
      .set("spark.executor.memory", "2048m")
      .set("spark.driver.memory", "2048m")

    val sc = new SparkContext(conf)
    val data = sc.textFile(inputDataDir+dataFileName)

    val parsedData = data.map { line =>
      val parts = line.split(',')
      var features = parts.tail


      var featureSeq:Seq[(Int,Double)] =  Seq[(Int,Double)]()
      var allFeaturesList:List[Int] = List.range(0,49)
      var excludedFeaturesList:List[Int] = List(8, 14,15,17,18, 20,21,22,23,24)
      var activeFeaturesList:List[Int]=
      allFeaturesList
        //allFeaturesList diff excludedFeaturesList
      //List(4, 8, 10, 12, 32)

      var featureLength: Int = activeFeaturesList.length

      var featurePosition = 0
            for  (i <- 0 to features.length - 1) {
              if (activeFeaturesList.contains(i)) {
                if (!features(i).isEmpty && features(i) != "NaN") {
                  var value: Double = features(i).toDouble
                  if (i == 0)
                      value = Math.round((value - 21)/10)
                  if (i==4)
                    if ( value > 0 ) value = 1

                  featureSeq = featureSeq :+(featurePosition, value)
                }
                featurePosition += 1

              }
            }

            var featureVector = Vectors.sparse(featureLength, featureSeq)
            //println(s"${featureLength}, ${featureSeq.length}")
            LabeledPoint(parts(0).toDouble, featureVector)
    }

   // val parsedData = MLUtils.loadLibSVMFile(sc, inputDataDir + libsvmDataFileName)
/*       val data = sc.textFile(inputDataDir+sampleDataFileName)

       val parsedData = data.map { line =>
         val parts = line.split(',')
         var features = parts.tail


         var featureSeq:Seq[(Int,Double)] =  Seq[(Int,Double)]()
         var featureLength: Int = features.length

               for  (i <- 0 to features.length - 1){
                 try {
                   var value:Double = features(i).toDouble
                   featureSeq = featureSeq :+ (i, value)
                   //println(featureSeq.toString())
                 }
                 catch {
                   case e: NumberFormatException => println("Exception")
                 }
               }

               var featureVector = Vectors.sparse(featureLength, featureSeq)
               //println(s"${featureLength}, ${featureSeq}")
               // println(featureVector.toString)
               LabeledPoint(parts(0).toDouble, featureVector)
       }*/

    val splits = parsedData.randomSplit(Array(0.7, 0.3))//, 13l)
    val (trainingData, testData) = (splits(0), splits(1))

    val svmAlg = new SVMWithSGD()
    svmAlg.optimizer.
      //setNumIterations(100). /*0.5596400488018982*/
      //setNumIterations(97). /* 0.46940163739296736*/
      //setNumIterations(96). /* 0.4668006673064466*/
      setNumIterations(95). /*0.5627041161723247*/
      //setNumIterations(94). /*0.48231024170330467*/
      //setNumIterations(93). /*0.4698135893945133*/

      //setNumIterations(90). /*0.48100743187448425*/
      setRegParam(0.1).
      setUpdater(new L1Updater)
    val model = svmAlg.run(trainingData)

        // Run training algorithm to build the model
        val numIterations = 75
        //val model = SVMWithSGD.train(trainingData, numIterations)

        // Clear the default threshold.
        model.clearThreshold()

        // Compute raw scores on the test set.
        val scoreAndLabels = testData.map { point =>
          val score = model.predict(point.features)
          (score, point.label)
        }

        // Get evaluation metrics.
        val metrics = new BinaryClassificationMetrics(scoreAndLabels)
        val auROC = metrics.areaUnderROC()

        println("Area under ROC = " + auROC)

    // Save and load model
    //model.save(sc, "target/tmp/myModelPath")
    //val sameModel = SVMModel.load(sc, "target/tmp/myModelPath")

/*    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 100 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 15
    val maxBins = 100
    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    // Compute raw scores on the test set.
    val scoreAndLabels = testData.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)
    println("Test Error = " + testErr)*/
   // println("Learned classification forest model:\n" + model.toDebugString)

 /*   // Save and load model
    model.save(sc, "target/tmp/myRandomForestClassificationModel")
    val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel")
*/  }
}
