package com.consumer

import java.sql.Timestamp
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._


case class OrderDetails(orderId: String, timestampUtc: Timestamp)
case class Order(orderType: String, data: OrderDetails)

class EventConsumer(sparkSession: SparkSession) {

  import org.apache.spark.sql.Encoders
  implicit val orderDetailsEncoder = Encoders.product[OrderDetails]
  implicit val orderEncoder = Encoders.product[Order]

  def processOrders(dataDirectory: String): Unit = {
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(5))
    ssc.start()
    ssc.awaitTermination()

    val orders = sparkSession.readStream.json(dataDirectory).as[Order].collect()
    countOrders(orders, (0,0,0))
  }

  private def countOrders(orders: Array[Order], counter: (Int, Int, Int)): (Int, Int, Int) = {
    if (orders.isEmpty) counter
    else {
      orders.head.orderType match {
        case "OrderCancelled" => countOrders(orders.tail, (counter._1+1, counter._2, counter._3))
        case "OrderAccepted" => countOrders(orders.tail, (counter._1, counter._2+1, counter._3))
        case "OrderPlaced" => countOrders(orders.tail, (counter._1, counter._2, counter._3+1))
      }
    }
  }

}
