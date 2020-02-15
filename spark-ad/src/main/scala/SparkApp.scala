import business.{AppStatProcessor, AreaStatProcessor, LogETLProcessor, ProvinceCityStatApp, ProvinceCityStatProcessor}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.commons.lang3.StringUtils

object SparkApp {
  /**
   *
   * 整个项目Spark作业入口点
   * 离线处理，一天一个批次
   *
   */
  def main(args: Array[String]): Unit = {
    var spark: SparkSession = SparkSession.builder().getOrCreate()

    /**
     * 入参统计
     * 1）spark.time
     * 2)spark.raw.path
     * 3)spark.ip.path
     */

    //spark-submit ... --conf spark.time=20181007
    var time: String = spark.sparkContext.getConf.get("spark.time")
    if(StringUtils.isBlank(time)){// 如果是空，后续代码就不应该执行
      logError("处理批次不能为空....")
      System.exit(0)
    }
    // STEP1: ETL
    LogETLProcessor.process(spark)

    // STEP2: 省份地市统计
    ProvinceCityStatProcessor.process(spark)

    // STEP3: 地域分布情况统计
    AreaStatProcessor.process(spark)

    // STEP4: APP分布情况统计
    AppStatProcessor.process(spark)

    spark.stop()
  }
}
