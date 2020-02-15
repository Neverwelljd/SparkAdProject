package business

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils._

object LogETLApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      master("local[*]")
      .appName("ETL")
      .getOrCreate()

    import spark.implicits._
    var jsonDF: DataFrame = spark.read.json("/home/ljd/Desktop/SparkAdProject/spark-ad/data/data-test.json")

    jsonDF.printSchema()

    var ipRowRDD: RDD[String] = spark.sparkContext.textFile("/home/ljd/Desktop/SparkAdProject/spark-ad/data/ip.txt")

    var ipDF: DataFrame = ipRowRDD.map(x => {
      val splits = x.split("\\|")
      val startIP = splits(2).toLong
      val endIP = splits(3).toLong
      val province = splits(6)
      val city = splits(7)
      val isp = splits(9)
      (startIP, endIP, province, city, isp)
    }).toDF("startIP","endIP","province","city","isp")

    ipDF.show(false)
    //TODO 需要将没一行日志中的IP獲得到对应的省份
    import org.apache.spark.sql.functions._

    def getLongIp() = udf((ip:String)=>{
      IPUtils.ip2Long(ip)
    })

    jsonDF = jsonDF.withColumn("ip_long",getLongIp()($"ip"))

    //jsonDF.join(ipDF,jsonDF("ip_long").between(ipDF("startIP"),ipDF("endIP"))).show(false)

    jsonDF.createOrReplaceTempView("logs")
    ipDF.createOrReplaceTempView("ips")

    var sql = SQLUtils.SQL
    var frame: DataFrame = spark.sql(sql)

    var tableName = DateUtils.getTableName("ods",spark)
    var masterAddresses = "localhost"
    val partitionID = "ip"
    KuduUtils.sink(frame,tableName, masterAddresses, SchemaUtils.ODSSchema, partitionID)

  }
}
