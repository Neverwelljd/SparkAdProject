package business

import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.{KuduUtils, SQLUtils, SchemaUtils}

object ProvinceCityStatApp {
  def main(args: Array[String]): Unit = {
    var spark: SparkSession = SparkSession.builder().master("local[*]").appName("ProvinceCityStatApp").getOrCreate()

    val sourceTableName = "ods"
    val masterAddresses = "localhost"

    var odsDF: DataFrame = spark.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", sourceTableName)
      .option("kudu.master", masterAddresses)
      .load()
    //odsDF.show(false)

    odsDF.createOrReplaceTempView("ods")
    var results: DataFrame = spark.sql(SQLUtils.PROVINCE_CITY_SQL)

    val sinkTableName = "province_city_stat"
    val partitionId = "provincename"

    KuduUtils.sink(results,sinkTableName,masterAddresses,SchemaUtils.ProvinceCitySchema,partitionId)
    spark.stop()

  }
}
