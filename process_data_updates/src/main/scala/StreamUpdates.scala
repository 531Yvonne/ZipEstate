import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

object StreamUpdates {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "localhost")

  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val table = hbaseConnection.getTable(TableName.valueOf("yvesyang_zip_estate"))
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamUpdates <brokers>
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamUpdates")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("yvesyang_data_updates")
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    // Get the lines, split them into words, count the words and print
    val serializedRecords = stream.map(_.value);
    val reports = serializedRecords.map(rec => mapper.readValue(rec, classOf[MarketRecord]))

    // How to write to an HBase table
    val batchStats = reports.map(wr => {
      val put = new Put(Bytes.toBytes(wr.record_key))
      put.addColumn(Bytes.toBytes("md"), Bytes.toBytes("year"), Bytes.toBytes(wr.year))
      put.addColumn(Bytes.toBytes("md"), Bytes.toBytes("month"), Bytes.toBytes(wr.month))
      put.addColumn(Bytes.toBytes("md"), Bytes.toBytes("zipcode"), Bytes.toBytes(wr.zipcode))
      put.addColumn(Bytes.toBytes("md"), Bytes.toBytes("property_type"), Bytes.toBytes(wr.property_type))
      put.addColumn(Bytes.toBytes("md"), Bytes.toBytes("median_sale_price"), Bytes.toBytes(wr.median_sale_price))
      put.addColumn(Bytes.toBytes("md"), Bytes.toBytes("median_list_price"), Bytes.toBytes(wr.median_list_price))
      put.addColumn(Bytes.toBytes("md"), Bytes.toBytes("homes_sold"), Bytes.toBytes(wr.home_sold))
      put.addColumn(Bytes.toBytes("md"), Bytes.toBytes("pending_sales"), Bytes.toBytes(wr.pending_sales))
      put.addColumn(Bytes.toBytes("md"), Bytes.toBytes("new_listings"), Bytes.toBytes(wr.new_listings))
      put.addColumn(Bytes.toBytes("md"), Bytes.toBytes("inventory"), Bytes.toBytes(wr.inventory))
      put.addColumn(Bytes.toBytes("md"), Bytes.toBytes("median_dom"), Bytes.toBytes(wr.median_dom))
      put.addColumn(Bytes.toBytes("md"), Bytes.toBytes("off_market_in_two_weeks"), Bytes.toBytes(wr.off_market_in_two_weeks))
      put.addColumn(Bytes.toBytes("md"), Bytes.toBytes("from_batch_layer"), Bytes.toBytes(false))
      table.put(put)
    })

    batchStats.print()
    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
