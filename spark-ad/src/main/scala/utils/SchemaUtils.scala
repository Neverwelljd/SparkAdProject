package utils
import org.apache.kudu.Schema
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{Schema, Type}
import scala.collection.JavaConverters._

object SchemaUtils {
  //1：初始化
  lazy val ODSSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("ip", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("sessionid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("advertisersid",Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adorderid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adcreativeid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adplatformproviderid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("sdkversion", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("adplatformkey", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("putinmodeltype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("requestmode", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adppprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("requestdate", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("appid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("appname", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("uuid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("device", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("client", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("osversion", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("density", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("pw", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("ph", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("provincename", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("cityname", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("ispid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("ispname", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("isp", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("networkmannerid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("networkmannername", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("iseffective", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("isbilling", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adspacetype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adspacetypename", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("devicetype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("processnode", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("apptype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("district", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("paymode", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("isbid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("winprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("iswin", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("cur", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rate", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("cnywinprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("imei", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("mac", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("idfa", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("openudid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("androidid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rtbprovince", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rtbcity", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rtbdistrict", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rtbstreet", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("storeurl", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("realip", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("isqualityapp", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidfloor", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("aw", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("ah", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("imeimd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("macmd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("idfamd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("openudidmd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("androididmd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("imeisha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("macsha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("idfasha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("openudidsha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("androididsha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("uuidunknow", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("userid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("iptype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("initbidprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adpayment", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("agentrate", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("lomarkrate", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adxrate", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("title", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("keywords", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("tagid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("callbackdate", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("channelid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("mediatype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("email", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("tel", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("sex", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("age", Type.STRING).nullable(false).build()
    ).asJava
    new Schema(columns)
  }


  lazy val ProvinceCitySchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("provincename",Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("cityname",Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("cnt",Type.INT64).nullable(false).key(true).build()
    ).asJava

    new Schema(columns)
  }


  lazy val AREA_SQL_STEP2 = "select provincename,cityname, " +
    "origin_request," +
    "valid_request," +
    "ad_request," +
    "bid_cnt," +
    "bid_success_cnt," +
    "bid_success_cnt/bid_cnt bid_success_rate," +
    "ad_display_cnt," +
    "ad_click_cnt," +
    "ad_click_cnt/ad_display_cnt ad_click_rate," +
    "ad_consumption," +
    "ad_cost from area_tmp " +
    "where bid_cnt!=0 and ad_display_cnt!=0"

  lazy val AREASchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("provincename",Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("cityname",Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("origin_request",Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("valid_request",Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("ad_request",Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bid_cnt",Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bid_success_cnt",Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bid_success_rate",Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("ad_display_cnt",Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("ad_click_cnt",Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("ad_click_rate",Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("ad_consumption",Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("ad_cost",Type.DOUBLE).nullable(false).build()
    ).asJava

    new Schema(columns)
  }


  lazy val APPSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("appid",Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("appname",Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("origin_request",Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("valid_request",Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("ad_request",Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bid_cnt",Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bid_success_cnt",Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bid_success_rate",Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("ad_display_cnt",Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("ad_click_cnt",Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("ad_click_rate",Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("ad_consumption",Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("ad_cost",Type.DOUBLE).nullable(false).build()
    ).asJava

    new Schema(columns)
  }
}
