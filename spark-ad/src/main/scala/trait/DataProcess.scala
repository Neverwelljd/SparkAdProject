package `trait`

import org.apache.spark.sql.SparkSession

trait DataProcess {
  def process(sparkSession: SparkSession)
}
