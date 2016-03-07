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

  def addValue(iPPacketsInfo: IPPacketsInfo, settings: Settings): Unit ={
    if (q.nonEmpty) {
      var queueFront = q.front

      while ((iPPacketsInfo.timeStamp - queueFront.timeStamp).milliseconds > settings.period * 1000) {
        totalCount -= queueFront.count
        totalWireLen -= queueFront.wirelen
        q.dequeue()
        queueFront = q.front
      }
    }
    totalCount += iPPacketsInfo.count
    totalWireLen += iPPacketsInfo.wirelen
    q.enqueue(iPPacketsInfo)
    checkBreach(settings)
  }

  def checkBreach(settings: Settings): Boolean = {
    var oldBreached:Boolean = breached
    if (((settings.limitType == 1) && (totalWireLen / totalCount > settings.value))
      || ((settings.limitType == 2) && (totalWireLen > settings.value)))
      breached = true
    else
      breached = false

    oldBreached != breached
  }
  override def toString: String = {
    s"$totalCount : $totalWireLen : $breached"
  }

}
