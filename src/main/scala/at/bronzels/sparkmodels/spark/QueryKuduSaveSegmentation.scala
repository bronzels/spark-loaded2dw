package at.bronzels.sparkmodels.spark

import at.bronzels.libcdcdw.myenum.AppModeEnum
import at.bronzels.libcdcdwbatch.dao.KuDuDao
import at.bronzels.libcdcdwbatch.util.MySparkUtil
import at.bronzels.sparkmodels.{CliInput, HtmlUtil, TextItemInput, WordUtil}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/*
import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
*/
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary
import com.hankcs.hanlp.tokenizer.StandardTokenizer

import scala.collection.JavaConverters._
import org.apache.spark.SparkConf

object QueryKuduSaveSegmented {
  val minTextLength = 48
  val minSegArrLength = 16

  def getStopRemoved(ss: SparkSession, df: DataFrame, colname: String, stopwords: Array[String]): DataFrame = {
    val bced_stopwords = ss.sparkContext.broadcast(stopwords)
    val stopremoved_udf = udf { (arr: Seq[String]) =>
      val stopremovedV = bced_stopwords.value
      arr.filter(word => {
        word != " " &&
          word != "\n" &&
          !(WordUtil.isNumber(word)) &&
          (WordUtil.isWithChinese(word) || WordUtil.isLetterDigit(word)) &&
          !stopremovedV.contains(word)
      }).toArray[String]
    }
    val retDF = df.withColumn(colname, stopremoved_udf(col(colname)))
    retDF.filter(size(retDF(colname)) > minSegArrLength)
  }

  /*
  def getSegmented(ss: SparkSession, df: DataFrame, colname: String): DataFrame = {
    val segmenter = new JiebaSegmenter()
    val bced_segmenter = ss.sparkContext.broadcast(segmenter)
    val jieba_udf = udf { (sentence: String) =>
      val segV = bced_segmenter.value
      segV.process(sentence.toString, SegMode.INDEX)
        .toArray().map(_.asInstanceOf[SegToken].word)
        .filter(_.length > 1).mkString("/")
    }
    df.withColumn(colname, jieba_udf(col(colname)))
  }
*/
  def getSegmented(ss: SparkSession, df: DataFrame, colname: String): DataFrame = {
    val jieba_udf = udf { (sentence: String) =>
      val list = StandardTokenizer.segment(sentence)
      CoreStopWordDictionary.apply(list)
      list.asScala.map(x => x.word.replaceAll(" ", "")).toArray[String]
    }
    df.withColumn(colname, jieba_udf(col(colname)))
  }

  def getTagFiltered(ss: SparkSession, df: DataFrame, colname: String): DataFrame = {
    val tagfiltered_udf = udf { (str: String) =>
      HtmlUtil.getTextFromHtml(str)
    }
    val retDF = df.withColumn(colname, tagfiltered_udf(col(colname)))
    retDF.filter(length(retDF(colname)) > minSegArrLength)
  }

  //spark-submit --master yarn --deploy-mode client --queue root.default --conf spark.core.connection.ack.wait.timeout=300 --conf spark.default.parallelism=40 --conf spark.driver.extraClassPath=/app/hadoop/hive/lib/hive-hbase-handler.jar:/app/hadoop/alluxio/client/spark/alluxio-1.6.1-spark-client.jar:/app/hadoop/spark_driver_jars/*:/app/hadoop/spark_shared_jars/* --conf spark.executor.extraClassPath=/app/hadoop/hive/lib/hive-hbase-handler.jar:/app/hadoop/alluxio/client/spark/alluxio-1.6.1-spark-client.jar:/app/hadoop/spark_executor_jars/*:/app/hadoop/spark_shared_jars/* --conf spark.core.connection.ack.wait.timeout=300 --conf spark.default.parallelism=40 --conf spark.yarn.executor.memoryOverhead=1g --driver-memory 10g --executor-memory 6g --executor-cores 1 --num-executors 5  --class at.bronzels.sparkmodels.spark.QueryKuduSaveSegmented /tmp/spark-models-1.0-SNAPSHOT.jar -kcu pro-hbase01:7052 -i dw_v_0_0_1_20191223_1830::strategy. -o segmented_ -D tablename=social_blog -D field_textid=body -D field_others=userid,createtime,area,origintype,displaylevel -D sql2filter='AuditStatus = 1 AND EntityStatus < 3'
  /*
  export HADOOP_USER_NAME=hadoop
  export SPARK_HOME=/mnt/u/spark
  */
  def main(args: Array[String]): Unit = {
    val newargs = Array[String](
      //"-am", "local",
      "-am", "remote",
      "-kcu", "pro-hbase01:7052",
      "-i", "dw_v_0_0_1_20191223_1830::strategy.",
      "-o", "segmented_",
      "-D", "tablename=social_blog",
      "-D", "field_textid=body",
      "-D", "field_others=userid,createtime,area,origintype,displaylevel",
      "-D", "sql2filter='AuditStatus = 1 AND EntityStatus < 3'"
    )

    val myCli = new CliInput
    val options = myCli.buildOptions()
    //if (myCli.parseIsHelp(options, args)) return
    if (myCli.parseIsHelp(options, newargs)) return

    var ss: SparkSession = null
    if (myCli.appModeEnum != null && myCli.appModeEnum.equals(AppModeEnum.LOCAL)) {
      ss = MySparkUtil.getHiveSupportLocalSparkSession(args)
    } else if (myCli.appModeEnum != null && myCli.appModeEnum.equals(AppModeEnum.REMOTE)) {
      val sparkConf = new SparkConf()
        .setMaster("yarn")
        .set("deploy-mode", "cluster")
        .set("yarn.resourcemanager.hostname", "pro-hbase01")
        .set("spark.yarn.queue", "root.default")
        .set("spark.driver.host", "192.168.3.4")
        .set("spark.core.connection.ack.wait.timeout", "300")
        .set("spark.default.parallelism", "40")
        .set("spark.yarn.executor.memoryOverhead", "1g")
        .set("spark.driver-memory", "10")
        .set("spark.executor-memory", "6g")
        .set("spark.executor-cores", "1")
        .set("spark.num-executors", "5")
        .set("spark.driver.extraClassPath", "/app/hadoop/spark_shared_jars/*")
        .set("spark.executor.extraClassPath", "/app/hadoop/spark_shared_jars/*")
        .setJars(List("/mnt/u/spark-models/target/spark-models-1.0-SNAPSHOT.jar"))
      ss = MySparkUtil.getSparkRemoteSession(args, sparkConf)
      //System.setProperty("HADOOP_USER_NAME", "hadoop")
      /*
      ss = SparkSession
        .builder()
        .appName(args.mkString(" "))
        .master("yarn")
        .config("deploy-mode", "cluster")
        .config("yarn.resourcemanager.hostname", "pro-hbase01")
        .config("spark.yarn.queue", "root.default")
        .config("spark.driver.host", "192.168.3.4")
        .config("spark.core.connection.ack.wait.timeout", "300")
        .config("spark.default.parallelism", "40")
        .config("spark.yarn.executor.memoryOverhead", "1g")
        .config("spark.driver-memory", "10")
        .config("spark.executor-memory", "6g")
        .config("spark.executor-cores", "1")
        .config("spark.num-executors", "5")
        .config("spark.driver.extraClassPath", "/app/hadoop/spark_shared_jars/*")
        .config("spark.executor.extraClassPath", "/app/hadoop/spark_shared_jars/*")
        .enableHiveSupport()
        .getOrCreate()
       */
    } else
      ss = MySparkUtil.getSparkSession(args)

    val kuduMaster = myCli.kuduConnUrl

    val textItemInput = new TextItemInput()
    for ((k, v) <- myCli.tables2Load) {
      println(k + " -> " + v)
      if (k.equals(TextItemInput.cliTableName))
        textItemInput.setTableName(v)
      if (k.equals(TextItemInput.cliFieldTextId))
        textItemInput.setFieldTextId(v)
      if (k.equals(TextItemInput.cliFieldOthers))
        textItemInput.setFieldOthers(v)
      if (k.equals(TextItemInput.cliSQL2Filter))
        textItemInput.setSql2Filter(v.replace("'", ""))
    }
    if (TextItemInput.cliTableName == null || TextItemInput.cliFieldTextId == null)
      throw new RuntimeException("mandatory input is not available")

    val kuduDF = KuDuDao.readKuduTable(ss, kuduMaster, myCli.inputPrefix + textItemInput.getTableName)
      .repartition(12)
    kuduDF.persist(StorageLevel.MEMORY_AND_DISK)
    //kuduDF.printSchema()
    kuduDF.show(5)
    System.out.println("kuduDF.count():" + kuduDF.count())

    var filteredDF = kuduDF
    if (textItemInput.sql2Filter != null) {
      filteredDF = filteredDF
        .filter(textItemInput.sql2Filter)
      //filteredDF.show(5)
      System.out.println("filteredDF.count():" + filteredDF.count())
    }

    var selectedDF = filteredDF
    if (textItemInput.fieldOthers != null) {
      val othersArr = textItemInput.fieldOthers.split(",")
      selectedDF = selectedDF
        .select(textItemInput.fieldTextId, othersArr: _*)
      //selectedDF.printSchema()
      //selectedDF.show(5)
    }

    val tagFilteredDF = getTagFiltered(ss, selectedDF, textItemInput.fieldTextId)
    //tagFilteredDF.show(5)
    tagFilteredDF.show()
    System.out.println("tagFilteredDF.count():" + tagFilteredDF.count())

    val segmentedDF = getSegmented(ss, tagFilteredDF, textItemInput.fieldTextId)
    segmentedDF.printSchema()
    segmentedDF.show(5)

    val in = QueryKuduSaveSegmented.getClass.getClassLoader.getResourceAsStream("stopwords.txt")
    val stopwords = scala.io.Source.fromInputStream(in).getLines.toArray
    System.out.println("stopwords:" + stopwords)
    val stopRemovedDF = getStopRemoved(ss, segmentedDF, textItemInput.fieldTextId, stopwords)
    stopRemovedDF.printSchema()
    stopRemovedDF.show(5)
    System.out.println("stopRemovedDF.count():" + stopRemovedDF.count())

    stopRemovedDF.repartition(6).write.format("orc").option("serialization.null.format", "").saveAsTable(myCli.outputPrefix + textItemInput.getTableName)
  }
}
