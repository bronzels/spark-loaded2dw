package at.bronzels

import at.bronzels.libcdcdwbatch.conf.MySQLEnvConf
import at.bronzels.libcdcdwbatch.dao.{KuDuDao, MysqlDao}
import at.bronzels.libcdcdwbatch.util.{CommonStat, MySparkUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StructField}

object QueryMysqlAndInsertKudu {
  def main(args: Array[String]): Unit = {
    val ss: SparkSession = MySparkUtil.getSparkSession(args)

    val fixSparkReadMysqlPartitionColumn = "id"
    val kuduMaster = "beta-hbase01:7051"
    /* (1, 2) 中 1 表示新加的 column 的 StructField, 2 表示 default 值, 用字符串表示，因为会用 cast 转换为 1 中指定的 dataType */
    val fixColumn2Add = Array(
      (StructField("_dwsyncts", LongType, nullable = true), System.currentTimeMillis().toString)
    )

    val mysqlAccountConf = new MySQLEnvConf("jdbc:mysql://10.0.0.46:3316/account?user=liuxiangbin&password=m1njooUE04vc&zeroDateTimeBehavior=convertToNull", "liuxiangbin", "m1njooUE04vc", "account")
    val mysqljsdConf = new MySQLEnvConf("jdbc:mysql://10.0.0.46:3316/jsd?user=liuxiangbin&password=m1njooUE04vc&zeroDateTimeBehavior=convertToNull", "liuxiangbin", "m1njooUE04vc","jsd")
    val mysqlCopytradingConf = new MySQLEnvConf("jdbc:mysql://10.0.0.46:3316/copytrading?user=liuxiangbin&password=m1njooUE04vc&zeroDateTimeBehavior=convertToNull", "liuxiangbin", "m1njooUE04vc", "copytrading")

    val url2DB2TablePksMap =  Map[MySQLEnvConf,Array[(String, String, Array[String])]](
      /*mysqlAccountConf -> Array(
        ("user_accounts", "presto::jsd_v1.insct_account_user_accounts", Array("id"))
      )*/
      /*mysqljsdConf -> Array(
        ("all_data_types", "presto::dw_v_0_0_1_20191108_1435.insct_jsd_all_data_types", Array("id"))
      )*/
      mysqlCopytradingConf -> Array(
        ("t_users", "presto::jsd_v1.insct_copytrading_t_users", Array("id")),
        ("t_followorder", "presto::jsd_v1.insct_copytrading_t_followorder", Array("id")),
        ("t_trades", "presto::jsd_v1.insct_copytrading_t_trades", Array("id"))
      )
    )

    url2DB2TablePksMap.foreach( u => {
      val mySQLEnvConf = u._1
      val table2KuduArr = u._2
      table2KuduArr.foreach(tableInfo => {
        val mysqlTable = tableInfo._1
        val kuduOutputTable = tableInfo._2
        val kuduTablePKs = tableInfo._3
        val rawMysqlDF = MysqlDao.getMysqlHugeTblDF(ss, mySQLEnvConf, mysqlTable, fixSparkReadMysqlPartitionColumn, 30)

        rawMysqlDF.schema.foreach(u => {
          println("field:%s, type:%s".format(u.name, u.dataType))
        })
        val afterDataTypeConvertDF = MysqlDao.convertMysqlDataType2DebeziumDataType(ss, rawMysqlDF, mySQLEnvConf, mysqlTable)
        var afterAddFixColumnsDF = afterDataTypeConvertDF
        afterAddFixColumnsDF  = afterAddFixColumnsDF
        fixColumn2Add.foreach( u => {
          afterAddFixColumnsDF = CommonStat.addColumnWithDefaultValue(afterAddFixColumnsDF, u._1, u._2)
        })

        //KuDuDao.createTable(ss, kuduMaster, kuduOutputTable, afterAddFixColumnsDF.schema, kuduTablePKs)
        //KuDuDao.dropTable(new KuduContext(kuduMaster, ss.sparkContext), kuduOutputTable)
        KuDuDao.saveFormatDF2Kudu(ss, kuduMaster, afterDataTypeConvertDF, kuduOutputTable, kuduTablePKs)
      })
    })
  }
}
