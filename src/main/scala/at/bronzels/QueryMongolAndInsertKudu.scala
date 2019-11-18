package at.bronzels

import at.bronzels.libcdcdwbatch.bean.sourceTableToKuduInfo
import at.bronzels.libcdcdwbatch.conf.MongoEnvConf
import at.bronzels.libcdcdwbatch.dao.{KuDuDao, MongoDao}
import at.bronzels.libcdcdwbatch.util.{CommonStat, MySparkUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StructField}

object QueryMongolAndInsertKudu {

def main(args: Array[String]): Unit = {
  val ss:SparkSession = MySparkUtil.getHiveSupportLocalSparkSession(args)

  val kudu_master = "10.1.0.11:7051"
  val kuduOutputPrefix = "presto::jsd.mongo_"
  val fixColumn2Add = Array(
    (StructField("_dwsyncts", LongType, nullable = true), System.currentTimeMillis().toString)
  )
  val bigDataMongoBaseUrl = "mongodb://root:BbEPgA6TFkaGNIp6@dds-wz96c88b733b36d433330.mongodb.rds.aliyuncs.com:3717/%s?authMechanism=SCRAM-SHA-1&authSource=admin"
  val cShapeBaseUrl = "mongodb://fmbetadb002:31Bawd0c5GEq@alibeta-mongo.followme-internal.com:27017/%s?authSource=admin"

  val url2DB2TablePksMap =  Map[String, Map[String, Array[(String, Array[String])]]](
    bigDataMongoBaseUrl -> Map(
      "strtest_jsd" ->
        Array(
          ("all_data_typs", Array("_id"))
          /*("mg_result_all", Array("_id")),
          ("mg_result_day", Array("_id")),
          ("mg_result_symall", Array("_id"))*/
        )
    )/*,
    cShapeBaseUrl -> Map(
      "social" ->
        Array(
          ("comment", Array("_id")),
          ("blog", Array("_id")),
          ("useraction", Array("_id"))
        )
    )*/
  )

  val mongoTable2ReadInfoArr: scala.collection.mutable.ArrayBuffer[sourceTableToKuduInfo] = scala.collection.mutable.ArrayBuffer.empty[sourceTableToKuduInfo]
    url2DB2TablePksMap.foreach(u => {
      val connUrl = u._1
      val database2TablePk = u._2
      val mongoTable2Read = getMongoTable2ReadArr(connUrl, database2TablePk, kuduOutputPrefix)
      mongoTable2ReadInfoArr.append(mongoTable2Read.toSeq:_*)
  })

  mongoTable2ReadInfoArr.foreach(exportInfo => {
    val mongoEnvConf = new MongoEnvConf(exportInfo.sourceConnUrl)
    val rawMongoDf = MongoDao.getMongoDF(mongoEnvConf, ss,  exportInfo.sourceTableName)
    var afterAddFixColumnDF = rawMongoDf

    fixColumn2Add.foreach( u => {
      afterAddFixColumnDF = CommonStat.addColumnWithDefaultValue(afterAddFixColumnDF, u._1, u._2)
    })

    val afterConvertDF = MongoDao.getDataTypeConvertDF(ss, afterAddFixColumnDF)
    KuDuDao.saveFormatDF2Kudu(ss, kudu_master, afterConvertDF, exportInfo.kuduOutputName, exportInfo.kuduPrimaryKeyFieldArr)
  })
}

  def getMongoTable2ReadArr(mongoUrl: String, dbTablesPrimaryKeyMap: Map[String, Array[(String, Array[String])]], outputPrefix: String) :Array[sourceTableToKuduInfo] = {
    val ret = scala.collection.mutable.ArrayBuffer.empty[sourceTableToKuduInfo]
    dbTablesPrimaryKeyMap.foreach(u => {
      val databaseName = u._1
      val tablePakArrList = u._2
      val connUrl = mongoUrl.format(databaseName)
      tablePakArrList.foreach(tableInfo => {
        val tableName = tableInfo._1
        val primaryKeyArr = tableInfo._2
        ret.append(new sourceTableToKuduInfo(connUrl, tableName, outputPrefix + tableName,  primaryKeyArr))
      })
    })
    ret.toArray
  }

}
