package com.producer

import java.sql.Timestamp
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.scalatest._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar


class EventProducerTest extends WordSpec with Matchers with MockitoSugar {

  val spark: SparkSession = SparkSession.builder
    .appName("TestApp")
    .master("local[*]")
    .getOrCreate

  val mockUtils = mock[Utils]
  val underTest = new EventProducer(spark, mockUtils)
  val testTime = new Timestamp(Calendar.getInstance.getTime.getTime)
  when(mockUtils.getTime).thenReturn(testTime)
  when(mockUtils.uuid).thenReturn("1")

  "Event Producer" should {
    "create a given number of orders" in {
      underTest.createOrders(10).collect().length should be(10)
    }
    "correctly create an order event" in {
      val result = underTest.createOrders(2).collect().head
      result.orderType should startWith("Order")
      result.data should be(OrderDetails("1", testTime))
    }
  }
}
