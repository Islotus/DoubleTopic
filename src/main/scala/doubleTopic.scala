import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
//Milliseconds, Minutes

object doubleTopic {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("DoubStreaming").setMaster("local[5]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,  Seconds(1))

    val topics1 = Set("testGps")
    val topics2 = Set("testMeter")
    val brokers = "master:9092,slave01:9092,slave02:9092,slave03:9092,slave04:9092,slave05:9092,slave06:9092,slave07:9092,slave08:9092,slave09:9092,slave10:9092,slave11:9092,slave12:9092,slave13:9092"

    //kafka查询参数  // smallest largest
    val kafkaParams = Map[String, String](
      "auto.offset.reset" -> "largest",
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder")

    //创建direct stream
    val gpsKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics1).map(_._2)
    val transKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics2).map(_._2)

    def gps_CT_Value(line:String)={
      val s=line.split(",")
      val key=s(0)
      val value=s(2)
      (key,value)
    }

    def trans_CT_Value(line:String)={
      val s=line.split(",")
      val key=s(0)
      val value=s(2)
      (key,value)
    }

    val mappedGps = gpsKafkaStream.map(gps_CT_Value)
    val mappedTrans = transKafkaStream.map(trans_CT_Value)

    //Minutes(1)    //Seconds(20)
    val windowedMappedGps = mappedGps.window(Seconds(4))
    val windowedMappedTrans = mappedTrans.window(Seconds(4))
    val joinedStream = windowedMappedGps.join(windowedMappedTrans)
    joinedStream.print()
    //joinedStream.saveAsTextFiles("/home/youjun/app/flumeCheckPoint/sparkJoinResult/joinResult")

//    val joinedMap = joinedStream.transform(rdd => rdd.values).map(e => (e._1, e._2.toInt))
//    val sumByCompany = joinedMap.reduceByKey(_+_)
//    sumByCompany.print()

    val groupTrans = mappedTrans.groupByKey()
    groupTrans.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
