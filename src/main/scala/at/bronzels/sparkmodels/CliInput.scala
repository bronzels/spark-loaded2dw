package at.bronzels.sparkmodels

import at.bronzels.libcdcdw.CommonCli
import org.apache.commons.cli.{Option, Options}

import scala.collection.JavaConversions

class CliInput extends CommonCli {
  var kuduConnUrl: String = _
  var tables2Load: Map[String, String] = Map()
  var inputPrefix: String = _
  var outputPrefix: String = _

  override def buildOptions(): Options = {
    val options = super.buildOptions()

    val kuduConnectUrl = new Option("kcu", "kuduConnectUrl", true, "kud connect url")
    kuduConnectUrl.setRequired(true);options.addOption(kuduConnectUrl)

    val tables2Load = new Option("D", "tables2Load", true, "raw tables primary keys")
    tables2Load.setArgs(2)
    tables2Load.setValueSeparator('=')
    tables2Load.setRequired(true);options.addOption(tables2Load)

    val inputPrefix = new Option("i", "inputPrefix", true, "input kudu table name prefix")
    inputPrefix.setRequired(true);options.addOption(inputPrefix)

    val outputPrefix = new Option("o", "outputPrefix", true, "output hive table name prefix")
    outputPrefix.setRequired(false);options.addOption(outputPrefix)

    options
  }

  override def parseIsHelp(options: Options, args: Array[String]): Boolean = {
    if(super.parseIsHelp (options, args))
      return true

    val comm = getCommandLine(options, args)

    if (comm.hasOption("kcu"))
      kuduConnUrl = comm.getOptionValue("kcu")

    if (comm.hasOption("D"))
      tables2Load = JavaConversions.propertiesAsScalaMap(comm.getOptionProperties("D")).toMap

    if (comm.hasOption("i"))
      inputPrefix = comm.getOptionValue("i")

    if (comm.hasOption("o"))
      outputPrefix = comm.getOptionValue("o")

    false

  }

}
