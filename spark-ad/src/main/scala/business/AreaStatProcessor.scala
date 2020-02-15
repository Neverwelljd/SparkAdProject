package business

import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.{DateUtils, KuduUtils, SQLUtils, SchemaUtils}

object AreaStatProcessor {
  override def process(spark:SparkSession):Unit = {
    val sourceTableName = DateUtils.getTableName("ods",spark)
    val masterAddresses = "localhost"

    var odsDF: DataFrame = spark.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", sourceTableName)
      .option("kudu.master", masterAddresses)
      .load()
    odsDF.createOrReplaceTempView("ods")

    var resultTmp: DataFrame = spark.sql(SQLUtils.AREA_SQL_STEP1)
    resultTmp.createOrReplaceTempView("area_tmp")

    var results: DataFrame = spark.sql(SQLUtils.AREA_SQL_STEP2)

    val sinkTableName = DateUtils.getTableName("area_stat",spark)
    val partitionID = "provincename"

    KuduUtils.sink(results,sinkTableName,masterAddresses,SchemaUtils.AREASchema, partitionID)

  }
}
