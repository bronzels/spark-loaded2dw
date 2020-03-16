package at.bronzels.spark

object QueryTSDBAndInsertKudu {
  /*def createSchema(StructFields: Array[StructField], pks: Array[String]):StructType = {
    val schemaList = scala.collection.mutable.ArrayBuffer.empty[StructField]
    StructFields.foreach(u => {
      schemaList.append(u)
    })
    StructType(schemaList.toList)

  //def createSchema(field2Type: Map[String, DataType], pks: Array[String]):StructType = {
    /*
    val schemaList = scala.collection.mutable.ArrayBuffer.empty[StructField]
    field2Type.foreach(u => {
      schemaList.append(StructField(u._1, u._2, nullable = if (pks.contains(u._1.toLowerCase)) false else true))
    })
    StructType(schemaList.toList)*/
  }



  def main(args: Array[String]): Unit = {

    /* 连接 url, 默认不指定 database */
    val bigDataMongoConnUrl = "mongodb://root:BbEPgA6TFkaGNIp6@dds-wz96c88b733b36d433330.mongodb.rds.aliyuncs.com:3717/%s?authMechanism=SCRAM-SHA-1&authSource=admin"
    //val bigDataMongoDatabaseArr = ("datastatistic_str_0_0_0_22")
    /* 库名 -> () */
    val bigDataMongoDatabaseArr = Map(
      "datastatistic_str_0_0_0_22" -> ("mg_result_all", Array("login", "brokerid")),
      "mg_result_day" -> ("mg_result_day", Array("login", "brokerid", "close_date"))
    )

    val fixColumn2Add = Array(
      StructField("brokerid", IntegerType, nullable = false),
      StructField("login", StringType, nullable = false),
      StructField("seriests", LongType, nullable = false),
      StructField("standardlots_close", DoubleType, nullable = true),
      StructField("point_cs", DoubleType, nullable = true),
      StructField("deal_cs", LongType, nullable = true),
      StructField("deal_cf", LongType, nullable = true),
      StructField("point_close", DoubleType, nullable = true),

      StructField("standardlots_cs", DoubleType, nullable = true),
      StructField("time_possession", DoubleType, nullable = true),
      StructField("point_cf", DoubleType, nullable = true),
      StructField("withdraw", DoubleType, nullable = true),
      StructField("standardlots_cf", DoubleType, nullable = true),
      StructField("money_followed_close_actual", DoubleType, nullable = true),
      StructField("money_cs", DoubleType, nullable = true),
      StructField("deposit", DoubleType, nullable = true),
      StructField("money_close", DoubleType, nullable = true),
      StructField("money_cf", DoubleType, nullable = true),
      StructField("deal_close", LongType, nullable = true)

    )

    // e.g: cSharpe mongoDb url
    val cshMongoUrl = ""

    val finalFieldTypeMap = scala.collection.mutable.HashMap.empty[String, DataType]
    val fixFieldTypeMap: Map[String, DataType] = Map("brokerid" -> IntegerType, "login" -> StringType, "seriests" -> LongType)
    fixFieldTypeMap.foreach(u => finalFieldTypeMap.put(u._1, u._2))

    val conditionFieldTaskArrMap =  Map(
        CONDITION_non_filtered ->
          Map(
            "time_possession" -> Array[Int](fmcommonConstants.TASK_ALL),
            "deposit" -> Array[Int](fmcommonConstants.TASK_ALL),
            "withdraw" -> Array[Int](fmcommonConstants.TASK_ALL)
          ),
        CONDITION_close ->
          Map(
            "money" -> Array[Int](fmcommonConstants.TASK_ALL),
            "deal" -> Array[Int](fmcommonConstants.TASK_ALL),
            "point" -> Array[Int](fmcommonConstants.TASK_ALL),
            "standardlots" -> Array[Int](fmcommonConstants.TASK_ALL)
          ),
        CONDITION_cf ->
          Map(
            "money" -> Array[Int](fmcommonConstants.TASK_ALL),
            "point" -> Array[Int](fmcommonConstants.TASK_ALL),
            "standardlots" -> Array[Int](fmcommonConstants.TASK_ALL),
            "deal" -> Array[Int](fmcommonConstants.TASK_ALL)
          ),
        CONDITION_cs ->
          Map(
            "money" -> Array[Int](fmcommonConstants.TASK_ALL),
            "point" -> Array[Int](fmcommonConstants.TASK_ALL),
            "standardlots" -> Array[Int](fmcommonConstants.TASK_ALL),
            "deal" -> Array[Int](fmcommonConstants.TASK_ALL)
          ),
        CONDITION_USERTYPE_ACTUAL ->
          Map(
            "money_followed_close" -> Array[Int](fmcommonConstants.TASK_ALL)
          )
      )

    conditionFieldTaskArrMap.foreach(u => {
      val condition = u._1
      val fieldArr = u._2.keys

      var conditionPrefix = FMCondition.condition2SuffixMap.getOrElse(condition, null)
      if (conditionPrefix == null)
        conditionPrefix = ""
      val fieldWithPrefixArr = fieldArr.map(u => MyString.concatBySkippingEmpty("_", u, conditionPrefix))
      fieldWithPrefixArr.foreach(finalFieldTypeMap.put(_, DoubleType))
    })


    val pks = Array("brokerid", "login", "seriests")
    val schema = createSchema(fixColumn2Add, pks)
    val ss:SparkSession = MySparkUtil.getLocalSparkSession(args)
    val kudu_master = "beta-hbase01:7051"
    val kuduContext:KuduContext = new KuduContext(kudu_master, ss.sparkContext)
    val kuduTableName = "presto::dw_v_0_0_1_20191111_1535.insbd_datastatistic_tsdb_lxb_result_all"

    KuDuDao.createTable(kuduContext, kuduTableName, schema, pks)

  }*/

}
