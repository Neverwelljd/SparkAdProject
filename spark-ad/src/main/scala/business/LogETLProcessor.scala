package business

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.{DateUtils, IPUtils, KuduUtils, SQLUtils, SchemaUtils}

object LogETLProcessor {
  override def process(spark: SparkSession): Unit = {
    // 日志数据： 使用Data Source API直接加载要处理的json数据

    val rawPath: String = spark.sparkContext.getConf.get("spark.raw.path")

    var jsonDF: DataFrame = spark.read.json(rawPath)

    //    var jsonDF: DataFrame = spark.read.json("file:///Users/rocky/IdeaProjects/imooc-workspace/sparksql-train/data/data-test.json")
    //jsonDF.printSchema()
    //jsonDF.show(false)


    import spark.implicits._

    val ipRulePath: String = spark.sparkContext.getConf.get("spark.ip.path")
    val ipRowRDD: RDD[String] = spark.sparkContext.textFile(ipRulePath)

    //    val ipRowRDD: RDD[String] = spark.sparkContext.textFile("file:///Users/rocky/IdeaProjects/imooc-workspace/sparksql-train/data/ip.txt")

    // 建议使用DF  需要将RDD转成DF ==> DF的相关操作  或者DF注册成表 然后进行相关操作
    val ipRuleDF: DataFrame = ipRowRDD.map(x => {
      val splits: Array[String] = x.split("\\|")
      val startIP: Long = splits(2).toLong
      val endIP: Long = splits(3).toLong
      val province: String = splits(6)
      val city: String = splits(7)
      val isp: String = splits(9)
      (startIP, endIP, province, city, isp)
    }).toDF("start_ip", "end_ip", "province", "city", "isp")
    //ipRuleDF.show(false)

    // TODO 需要将每一行日志中的ip获得到对应的省份、城市、运营商

    // 两个DF进行join，条件是json中的ip 是在规则ip中的范围内就行 ip between ... and ...
    // TODO... json中的ip转换一下  通过前面我们学习的Spark SQL UDF函数
    import org.apache.spark.sql.functions._

    def getLongIp() = udf((ip:String) => {
      IPUtils.ip2Long(ip)
    })

    jsonDF = jsonDF.withColumn("ip_long", getLongIp()($"ip"))

    //    jsonDF.join(ipRuleDF,jsonDF("ip_long")
    //      .between(ipRuleDF("start_ip"), ipRuleDF("end_ip")))
    //      .show(false)

    // TODO... 你知道join有哪几种类型，区别是什么
    jsonDF.createOrReplaceTempView("logs")
    ipRuleDF.createOrReplaceTempView("ips")

    // ETL处理完之后，肯定要落地到某个地方 KUDU
    val sql = SQLUtils.SQL
    val result: DataFrame = spark.sql(sql)

    // ===> printSchema

    // 重构： Client result  tableName  master  schema  partitionId


    // 只需要定义表相关的信息，剩下的创建表 删除表操作全部封装到KuduUtils的Sink方法中
    val tableName = DateUtils.getTableName("ods", spark)
    val masterAddresses = "localhost"
    val partitionId = "ip"

    KuduUtils.sink(result,tableName,masterAddresses,SchemaUtils.ODSSchema, partitionId)

  }
}
