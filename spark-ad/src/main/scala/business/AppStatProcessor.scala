package business

import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.{DateUtils, KuduUtils, SQLUtils, SchemaUtils}

object AppStatProcessor {
  override def process(spark:SparkSession): Unit ={
    val sourceTableName = DateUtils.getTableName("ods", spark)
    val masterAddresses = "localhost"

    var odsDF: DataFrame = spark.read.format("org.apache.spark.kudu")
      .option("kudu.table", sourceTableName)
      .option("kudu.master", masterAddresses)
      .load()
    odsDF.createOrReplaceTempView("ods")
    var resultTmp: DataFrame = spark.sql(SQLUtils.APP_SQL_STEP1)
    resultTmp.createOrReplaceTempView("app_tmp")

    var result: DataFrame = spark.sql(SQLUtils.APP_SQL_STEP2)

    val sinkTableName = DateUtils.getTableName("app_stat", spark)
    val partitionId = "appid"

    KuduUtils.sink(result,sinkTableName,masterAddresses,SchemaUtils.APPSchema, partitionId)


  }
}