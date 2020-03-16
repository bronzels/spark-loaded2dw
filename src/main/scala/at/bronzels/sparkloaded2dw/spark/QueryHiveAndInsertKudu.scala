package at.bronzels.sparkloaded2dw.spark

import at.bronzels.libcdcdwbatch.dao.KuDuDao
import at.bronzels.libcdcdwbatch.util.MySparkUtil
import at.bronzels.sparkloaded2dw.CliInput
import org.apache.spark.sql.SparkSession

object QueryHiveAndInsertKudu {
  def main(args: Array[String]): Unit = {
    val myCli = new CliInput
    val options = myCli.buildOptions()
    if (myCli.parseIsHelp(options, args)) return
    val ss: SparkSession = MySparkUtil.getHiveSupportLocalSparkSession(args)

    val kuduMaster = myCli.kuduConnUrl

    val table2Load= Array(("h_2_0_4_2_mt4trades_jsd", Array("brokerid", "login","ticket")))
    //val outputPrefix = myCli.outputPrefix
    val outputPrefix = "jsd::"

    table2Load.foreach(table => {
      val selectDF = ss.read.table(table._1)
      KuDuDao.saveFormatDF2Kudu(ss, kuduMaster, selectDF, outputPrefix + table._1, table._2)
    })

  }

}
