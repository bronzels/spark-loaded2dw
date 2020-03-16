package at.bronzels.sparkloaded2dw.spark

import at.bronzels.libcdcdwbatch.conf.MySQLEnvConf
import at.bronzels.libcdcdwbatch.dao.{KuDuDao, MysqlDao}
import at.bronzels.libcdcdwbatch.util.CommonStat.StructFieldWHDefaultValue
import at.bronzels.libcdcdwbatch.util.{CommonStat, MySparkUtil}
import at.bronzels.sparkloaded2dw.CliInput
import org.apache.spark.sql.types.{LongType, StructField}
import org.apache.spark.sql.{DataFrame, SparkSession}

object QueryMysqlAndInsertKudu {
  def main(args: Array[String]): Unit = {

    val myCli = new CliInput
    val options = myCli.buildOptions()
    if (myCli.parseIsHelp(options, args)) return

    val ss: SparkSession = MySparkUtil.getLocalSparkSession(args)
    val isSrcFieldNameWTUpperCase: Boolean = myCli.isSrcFieldNameWTUpperCase
    val fromHive: Boolean = myCli.sourceFromHive
    val kuduMaster = myCli.kuduConnUrl
    val table2KuduArr = myCli.tables2load
    val outputPrefix = myCli.outputPrefix
    val mysqlUrl = "jdbc:mysql://%s/%s?user=%s&password=%s&zeroDateTimeBehavior=convert_to_null".format(myCli.dbConnUrl, myCli.dbName, myCli.dbUser, myCli.dbPassword)
    val mySQLEnvConf: MySQLEnvConf = new MySQLEnvConf(mysqlUrl, myCli.dbUser, myCli.dbPassword, myCli.dbName)

    /* (1, 2) 中 1 表示新加的 column 的 StructField, 2 表示 default 值, 用字符串表示，因为会用 cast 转换为 1 中指定的 dataType */
    val fixColumn2Add = Array(
      StructFieldWHDefaultValue(StructField("_dwsyncts", LongType, nullable = true), System.currentTimeMillis().toString)
    )

    table2KuduArr.foreach(tableName => {
      //val kuduOutputTable = outputPrefix + myCli.dbName + "_" + tableName.toLowerCase
      val kuduOutputTable = outputPrefix + tableName.toLowerCase
      val hiveOutputTable = kuduOutputTable.replace("::", "_").replace(".", "_")
      var afterAddFixColumnsDF: DataFrame = null
      val tablePKs = MysqlDao.getMysqlTablePKFields(ss, mySQLEnvConf, tableName)
      tablePKs.foreach(u => {
        println("table: %s, pk: %s".format(tableName, u))
      })
      if(fromHive){
        afterAddFixColumnsDF = ss.read.table(hiveOutputTable)
        val dfSchema = afterAddFixColumnsDF.schema
        tablePKs.foreach(field => {
          if (!dfSchema.names.contains(field))
            afterAddFixColumnsDF = afterAddFixColumnsDF.withColumnRenamed(field.toLowerCase, field)
        })
        afterAddFixColumnsDF = MysqlDao.convertMysqlDataType2DebeziumDataType(ss, afterAddFixColumnsDF, mySQLEnvConf, tableName)
        afterAddFixColumnsDF = CommonStat.addColumnWithDefaultValue(afterAddFixColumnsDF, fixColumn2Add)
      }else{
        val rawMysqlDF = MysqlDao.getMysqlHugeTblDF(ss, mySQLEnvConf, tableName, tablePKs, 60)
        afterAddFixColumnsDF = CommonStat.addColumnWithDefaultValue(rawMysqlDF, fixColumn2Add)
      }

      afterAddFixColumnsDF.schema.foreach( u => {
        println("field:%s, type:%s".format(u.name, u.dataType.typeName))
      })
      KuDuDao.saveFormatDF2Kudu(ss, kuduMaster, afterAddFixColumnsDF, kuduOutputTable, tablePKs, isSrcFieldNameWTUpperCase)
    })
  }


}
