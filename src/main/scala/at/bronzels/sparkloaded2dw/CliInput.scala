package at.bronzels.sparkloaded2dw

import at.bronzels.libcdcdw.CommonCli
import org.apache.commons.cli.{Option, Options}

class CliInput extends CommonCli {

  val defaultMysqlTablePk = "id"
  val defaultMongoTablePk = "_id"

  var dbConnUrl: String = _
  var dbName: String = _
  var dbUser: String = _
  var dbPassword: String = _
  var tables2load: Array[String] = _
  var outputPrefix: String = _
  var isSrcFieldNameWTUpperCase: Boolean = _
  var kuduConnUrl: String = _
  var tablePrimaryKeysArr: Array[String] = _
  var sourceFromHive: Boolean = false
  val primaryKeys2TableMap: scala.collection.mutable.HashMap[String,String] = scala.collection.mutable.HashMap.empty[String,String]

  override def buildOptions(): Options = {
    val options = super.buildOptions()

    val connectHostName = new Option("dcu", "connectionUrl", true, "connect db url: hostName:port")
    connectHostName.setRequired(true);options.addOption(connectHostName)

    val databaseName = new Option("dn", "databaseName", true, "connect database name")
    databaseName.setRequired(true);options.addOption(databaseName)

    val databaseUser = new Option("du", "databaseUser", true, "database user name")
    databaseUser.setRequired(true);options.addOption(databaseUser)

    val databasePassWD = new Option("dp", "databasePassWD", true, "database user password ")
    databasePassWD.setRequired(true);databasePassWD.setRequired(true);options.addOption(databasePassWD)

    val tables2LoadWHSpark = new Option("t2l", "table2load", true, "raw tables to load witt spark")
    tables2LoadWHSpark.setRequired(true);options.addOption(tables2LoadWHSpark)

    val tables2LoadPKs = new Option("tpks", "table 2 load primaryKeys ", true, "raw tables primary keys")
    tables2LoadPKs.setRequired(false);options.addOption(tables2LoadPKs)

    val outputPrefix = new Option("o", "output", true, "output table name prefix")
    options.addOption(outputPrefix)

    val isSrcFieldNameWTUpperCase = new Option("sfuc", "sourceFieldWIUperCase", true, "is source fields with upperCase")
    options.addOption(isSrcFieldNameWTUpperCase)

    val kuduConnectUrl = new Option("kcu", "kuduConnectUrl", true, "kud connect url")
    options.addOption(kuduConnectUrl)

    val sourceFromHive = new Option("fh", "dataSourceFromHive", true, "is datasource from hive")
    options.addOption(sourceFromHive)


    options
  }

  override def parseIsHelp(options: Options, args: Array[String]): Boolean = {
    if(super.parseIsHelp (options, args))
      return true

    val comm = getCommandLine(options, args)

    if (comm.hasOption("dcu")) {
      dbConnUrl = comm.getOptionValue("dcu")
    }

    if (comm.hasOption("dn"))
       dbName = comm.getOptionValue("dn")

    if (comm.hasOption("du"))
      dbUser = comm.getOptionValue("du")

    if (comm.hasOption("dp"))
      dbPassword = comm.getOptionValue("dp")

    if (comm.hasOption("fh")) {
      val inputStr = comm.getOptionValue("fh")
      if (inputStr.equals("true") || inputStr.equals("1"))
        sourceFromHive = true
      else
        sourceFromHive = false
    }

    if (comm.hasOption("t2l")) {
      tables2load = comm.getOptionValue("t2l").split(",")
      if (comm.hasOption("tpks")){
        val tablePks = comm.getOptionValue("tpks").split(",")
        tables2load.zip(tablePks).toMap.foreach(u => {
          primaryKeys2TableMap.put(u._1, u._2)
          println(primaryKeys2TableMap)
        })
      }
    }

    if (comm.hasOption("o"))
      outputPrefix = comm.getOptionValue("o")

    if (comm.hasOption("sfuc")){
      val inputValue = comm.getOptionValue("sfuc")
      if (inputValue.equals("true") || inputValue.equals("0"))
        isSrcFieldNameWTUpperCase = true
      else isSrcFieldNameWTUpperCase = false
    }

    if (comm.hasOption("kcu"))
      kuduConnUrl = comm.getOptionValue("kcu")

    false

  }

}
