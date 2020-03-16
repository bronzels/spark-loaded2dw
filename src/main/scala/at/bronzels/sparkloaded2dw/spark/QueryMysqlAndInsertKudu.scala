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

    val ss: SparkSession = MySparkUtil.getSparkSession(args)
    val isSrcFieldNameWTUpperCase: Boolean = myCli.isSrcFieldNameWTUpperCase
    val fromHive: Boolean = myCli.sourceFromHive
    val kuduMaster = myCli.kuduConnUrl
    val table2KuduArr = myCli.tables2load
    val outputPrefix = myCli.outputPrefix
    val mysqlUrl = "jdbc:mysql://%s/%s?user=%s&password=%s&zeroDateTimeBehavior=convertToNull".format(myCli.dbConnUrl, myCli.dbName, myCli.dbUser, myCli.dbPassword)
    val mySQLEnvConf: MySQLEnvConf = new MySQLEnvConf(mysqlUrl, myCli.dbUser, myCli.dbPassword, myCli.dbName)

    /* (1, 2) 中 1 表示新加的 column 的 StructField, 2 表示 default 值, 用字符串表示，因为会用 cast 转换为 1 中指定的 dataType */
    val fixColumn2Add = Array(
      StructFieldWHDefaultValue(StructField("_dwsyncts", LongType, nullable = true), System.currentTimeMillis().toString)
    )

    table2KuduArr.foreach(tableName => {
      //val kuduOutputTable = outputPrefix + myCli.dbName + "_" + tableName.toLowerCase
      val kuduOutputTable = outputPrefix + tableName.toLowerCase
      val hiveOutputTable = kuduOutputTable.replace("::", "_").replace(".", "_")
      var sourceTableDF: DataFrame = null
      val tablePKs = MysqlDao.getMysqlTablePKFields(ss, mySQLEnvConf, tableName)
      tablePKs.foreach(u => {
        println("table %s to load, table pks: %s".format(tableName, u))
      })

      if(fromHive)
        sourceTableDF = ss.read.table(hiveOutputTable)
      else
        sourceTableDF = MysqlDao.getMysqlHugeTblDF(ss, mySQLEnvConf, tableName, tablePKs, 60)

      var formatDataTypeDF = sourceTableDF
      val dfSchema = sourceTableDF.schema
      tablePKs.foreach(field => {
        if (!dfSchema.names.contains(field))
          formatDataTypeDF = formatDataTypeDF.withColumnRenamed(field.toLowerCase, field)
      })
      formatDataTypeDF = MysqlDao.convertMysqlDataType2DebeziumDataType(ss, formatDataTypeDF, mySQLEnvConf, tableName)
      formatDataTypeDF = CommonStat.addColumnWithDefaultValue(formatDataTypeDF, fixColumn2Add)

      formatDataTypeDF.schema.foreach( u => {
        println("table: %s, field:%s, type:%s".format(tableName, u.name, u.dataType.typeName))
      })
      KuDuDao.saveFormatDF2Kudu(ss, kuduMaster, formatDataTypeDF, kuduOutputTable, tablePKs, isSrcFieldNameWTUpperCase)
    })
  }


}
