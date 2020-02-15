package utils

import java.util

import org.apache.kudu.client
import org.apache.kudu.client.{CreateTableOptions, KuduClient}
import org.apache.kudu.client.KuduClient.KuduClientBuilder
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.catalyst.ScalaReflection.Schema

object KuduUtils {
  /**
   * 将数据写到kudu上
   * @param data  DataFrame结果集合
   * @param tablename Kudu目标表
   * @param master Kuda的Master地址
   * @param schema Kudu表的schema
   * @param partitionID Kudu表的分区字段
   */
  def sink(data:DataFrame,
           tablename:String,
           master:String,
           schema:Schema,
           partitionID:String
          ):Unit = {
    var client: KuduClient = new KuduClientBuilder(master).build()

    var options: CreateTableOptions = new client.CreateTableOptions()

    options.setNumReplicas(1)


    var parcols: util.LinkedList[String] = new util.LinkedList[String]()
    parcols.add(partitionID)
    options.addHashPartitions(parcols, 3)

    // 创建表
    if(client.tableExists(tablename)){
      client.deleteTable(tablename)
    }

    client.createTable(tablename, schema, options)

    //数据写入kudu
    data.write.mode(SaveMode.Append)
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.table",tablename)
      .option("kudu.master",master)
      .save()

  }
}
