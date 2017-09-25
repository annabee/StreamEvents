package com.producer

import java.sql.Timestamp

import org.apache.spark.sql.{Dataset, SparkSession}

case class OrderDetails(orderId: String, timestampUtc: Timestamp)
case class Order(orderType: String, data: OrderDetails)

class EventProducer(sparkSession: SparkSession, utils: Utils) {

  import sparkSession.implicits._

  def produceOrders(numberOfOrders: Int, batchSize: Int, interval: Int, outputDirectory: String): Unit = {
    var pendingOrders = numberOfOrders
    if (pendingOrders < batchSize)
        createOrders(pendingOrders).coalesce(1).write.json(s"/$outputDirectory/orders-${utils.getTime}.json")
    else {
      createOrders(batchSize).coalesce(1).write.json(s"$outputDirectory/orders-${utils.getTime}.json")
      Thread.sleep(interval*1000)
      pendingOrders -= batchSize
      produceOrders(pendingOrders, batchSize, interval, outputDirectory)
    }
  }

  private[producer] def createOrders(numberOfOrders: Int): Dataset[Order] = {
    (1 to math.floor(numberOfOrders/2).toInt).toList.flatMap { _ =>
    val (id, time) = (utils.uuid, utils.getTime)
    val placed = Order("OrderPlaced", OrderDetails(id, time))
    // for every 5 orders, 4 are accepted, 1 is cancelled - using stats to simulate that
    if (scala.util.Random.nextDouble() >= 0.2)
      Seq(Order("OrderAccepted", OrderDetails(id, time)), placed)
    else
      Seq(Order("OrderCancelled", OrderDetails(id, time)), placed)
    }.toDS()
  }
}


/**
  * Small utilities class. Makes life easier when stubbing responses in tests.
  */
class Utils {
  import java.util.Calendar
  def uuid = java.util.UUID.randomUUID.toString
  def getTime = new Timestamp(Calendar.getInstance.getTime.getTime)
}
