import com.consumer.EventConsumer
import org.apache.spark.sql.SparkSession

object ConsumerApp {

  def consumeEvents(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("Tags")
      .master("local[*]")
      .getOrCreate
    new EventConsumer(spark).processOrders("")
  }
}
