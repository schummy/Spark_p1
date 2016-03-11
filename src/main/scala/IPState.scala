import scala.collection.mutable
import scala.collection.mutable.Queue
/**
  * Created by Andrii_Krasnolob on 3/7/2016.
  */
class IPState
  extends Serializable {
  var q:Queue[IPPacketsInfo] = new mutable.Queue[IPPacketsInfo]
  var totalCount = 0L
  var totalWireLen = 0
  var breached:Boolean = false

  def addValue(iPPacketsInfo: IPPacketsInfo, settings: IPLimits): Unit ={
    println("-"*30 + q.length + "-"*30)
    if (q.nonEmpty) {

      while (q.nonEmpty &&
        (iPPacketsInfo.timeStamp -  q.front.timeStamp).milliseconds > settings.getPeriod(1) * 1000 ) {
        totalCount -=  q.front.count
        totalWireLen -=  q.front.wirelen
        q.dequeue()

      }
    }
    totalCount += iPPacketsInfo.count
    totalWireLen += iPPacketsInfo.wirelen
    q.enqueue(iPPacketsInfo)
    checkBreach(settings)
  }

  def checkBreach(settings: IPLimits): Boolean = {
    var oldBreached = breached

    if (((totalWireLen / totalCount > settings.getValue(1))) )
     // || ((settings.limitType == 2) && (totalWireLen > settings.value)))
      breached = true
    else
      breached = false


    oldBreached != breached
  }
  override def toString: String = {
    s"$totalCount : $totalWireLen : $breached"
  }

}
