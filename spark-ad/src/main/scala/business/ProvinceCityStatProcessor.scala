package business

import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.{DateUtils, KuduUtils, SQLUtils, SchemaUtils}

object ProvinceCityStatProcessor {
  override def process(spark:SparkSession):Unit = {
    // read data from kudu, then grouped by province and city

    val sourceTableName = DateUtils.getTableName("ods",spark)

    val masterAddresses = "local"

    var odsDF: DataFrame = spark.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", sourceTableName)
      .option("kudu.master", masterAddresses)
      .load()
    odsDF.createOrReplaceTempView("ods")
    var resutls: DataFrame = spark.sql(SQLUtils.PROVINCE_CITY_SQL)

    val sinkTableName = DateUtils.getTableName("province_city_stat", spark)
    val partitionId = "provincename"

    KuduUtils.sink(resutls, sinkTableName, masterAddresses, SchemaUtils.ProvinceCitySchema, partitionId)



  }


}
