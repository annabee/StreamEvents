import com.producer.{EventProducer, Utils}
import scopt.OptionParser
import org.apache.spark.sql.SparkSession

object ProducerApp {

  def main(args: Array[String]): Unit = {

    case class Config(numberOfOrders: Int = 0, batchSize: Int = 0, interval: Int = 0, outputDirectory: String = ".")
    val parser = new OptionParser[Config]("scopt") {
      head("generate-events", "1.0")

      opt[Int]("number-of-orders").action((x, c) =>
        c.copy(numberOfOrders = x)).text("number-of-orders is an integer property")
      opt[Int]("batch-size").action((x, c) =>
        c.copy(batchSize = x)).text("batch-size is an integer property")
      opt[Int]("interval").action((x, c) =>
        c.copy(interval = x)).text("interval is an integer property")
      opt[String]("output-directory").action((x, c) =>
        c.copy(outputDirectory = x)).text("output-directory is an string property")

      note("generate-events --number-of-orders=1000000 --batch-size=5000 --interval=1 --output-directory=<local-dir>")
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        println(config)
        val spark: SparkSession = SparkSession.builder
          .appName("Tags")
          .master("local[*]")
          .getOrCreate
        val producer = new EventProducer(spark, new Utils)
        producer.produceOrders(config.numberOfOrders, config.batchSize,config.interval, config.outputDirectory)
      case None =>
        parser.showUsage()
    }
  }
}
