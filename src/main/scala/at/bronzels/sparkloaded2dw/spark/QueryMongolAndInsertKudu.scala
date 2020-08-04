package at.bronzels.sparkloaded2dw.spark

import at.bronzels.libcdcdwbatch.conf.MongoEnvConf
import at.bronzels.libcdcdwbatch.dao.{KuDuDao, MongoDao}
import at.bronzels.libcdcdwbatch.util.CommonStat.StructFieldWHDefaultValue
import at.bronzels.libcdcdwbatch.util.{CommonStat, MySparkUtil}
import at.bronzels.sparkloaded2dw.Constants
import at.bronzels.sparkloaded2dw.{CliInput, Constants}
import org.apache.spark.sql.types.{LongType, StructField}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object QueryMongolAndInsertKudu {

def main(args: Array[String]): Unit = {

  val myCli = new CliInput
  val options = myCli.buildOptions()
  if (myCli.parseIsHelp(options, args)) return

  val ss: SparkSession = MySparkUtil.getSparkSession(args)
  val isSrcFieldNameWTUpperCase: Boolean = myCli.isSrcFieldNameWTUpperCase
  val kuduMaster = myCli.kuduConnUrl
  val table2KuduArr = myCli.tables2load
  val table2PrimaryKeysMap = myCli.primaryKeys2TableMap
  val outputPrefix = myCli.outputPrefix
  val fromHive: Boolean = myCli.sourceFromHive
  val mongoUrl = "mongodb://%s:%s@%s/%s?authMechanism=SCRAM-SHA-1&authSource=admin".format(myCli.dbUser, myCli.dbPassword, myCli.dbConnUrl, myCli.dbName)

  val fixColumn2Add = Array(
    StructFieldWHDefaultValue(StructField("_dwsyncts", LongType, nullable = true),System.currentTimeMillis().toString)
  )

  table2KuduArr.foreach(tableName => {
    //val kuduOutputTable = outputPrefix + "datastatistic_" + tableName.toLowerCase
    val kuduOutputTable = outputPrefix + { if(myCli.dbNameWNRev == null) myCli.dbName else myCli.dbNameWNRev } + "_" + tableName.toLowerCase
    val hiveOutputTable = kuduOutputTable.replace("::", "_").replace(".", "_")
    val mongoEnvConf = new MongoEnvConf(mongoUrl)
    var afterConvertDF: DataFrame = null

    if(fromHive){
      afterConvertDF = ss.read.table(hiveOutputTable)
    }else{
      val rawMongoDf = MongoDao.getMongoDF(mongoEnvConf, ss,  tableName)
      val afterAddFixColumnDF = CommonStat.addColumnWithDefaultValue(rawMongoDf, fixColumn2Add)
      afterConvertDF = MongoDao.getDataTypeConvertDF(ss, afterAddFixColumnDF)
      //afterConvertDF.write.mode(SaveMode.Overwrite).saveAsTable(hiveOutputTable)
    }

    val pkFields = table2PrimaryKeysMap.getOrElse(tableName, Constants.defaultMongoPrimaryKey)
    KuDuDao.saveFormatDF2Kudu(ss, kuduMaster, afterConvertDF, kuduOutputTable, Array(pkFields), isSrcFieldNameWTUpperCase)
    //afterConvertDF.write.mode("overwrite").saveAsTable(tableName)

  })
}

}
