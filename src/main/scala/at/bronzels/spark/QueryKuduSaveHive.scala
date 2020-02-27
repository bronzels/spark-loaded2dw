package at.bronzels.spark

import at.bronzels.CliInput
import at.bronzels.libcdcdwbatch.dao.{KuDuDao, MysqlDao}
import at.bronzels.libcdcdwbatch.udf.MysqlDataTypeConvertUdf
import at.bronzels.libcdcdwbatch.util.{CommonStat, MySparkUtil}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object QueryKuduSaveHive {


  def main(args: Array[String]): Unit = {
    val myCli = new CliInput
    val options = myCli.buildOptions()
    if (myCli.parseIsHelp(options, args)) return
    val ss: SparkSession = MySparkUtil.getHiveSupportLocalSparkSession(args)

    val kuduMaster = myCli.kuduConnUrl

    val table2Load = Array(("h_2_0_4_2_mt4trades_jsd", Array("brokerid", "login", "ticket")))
    val outputPrefix = "jsd::"
    table2Load.foreach(table => {
      val kuduDF = KuDuDao.readKuduTable(ss, kuduMaster, outputPrefix + table._1)
      kuduDF.repartition(6).write.format("orc").option("serialization.null.format", "").saveAsTable(table._1)
    })

  }
}
