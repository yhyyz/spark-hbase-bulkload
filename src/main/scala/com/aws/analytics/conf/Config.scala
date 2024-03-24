package com.aws.analytics.conf

case class Config(
                   env: String = "prod",
                   hbaseZK :String= "localhost:2181",
                   namespace:String = "default",
                   tableName:String = "",
                   rowKey:String ="",
                   cf:String ="",
                   columns:String ="",
                   sourceDir:String ="",
                   targetDir:String =""
                 )


object Config {


  def parseConfig(obj: Object, args: Array[String]): Config = {
    val programName = obj.getClass.getSimpleName.replaceAll("\\$", "")
    val parser = new scopt.OptionParser[Config]("spark " + programName) {
      head(programName, "1.0")
      opt[String]('e', "env").optional().action((x, config) => config.copy(env = x)).text("env: dev or prod")
      programName match {
        case "HBaseBulkLoad" =>
          opt[String]('z', "hbaseZK").required().action((x, config) => config.copy(hbaseZK = x)).text("hbaseZK,default: localhost:2181")
          opt[String]('s', "sourceDir").required().action((x, config) => config.copy(sourceDir = x)).text("sourceDir,source data")
          opt[String]('t', "targetDir").required().action((x, config) => config.copy(targetDir = x)).text("targetDir,dir for save hfile")
          opt[String]('n', "tableName").required().action((x, config) => config.copy(tableName = x)).text("tableName,hbase table name")
          opt[String]('k', "rowKey").required().action((x, config) => config.copy(rowKey = x)).text("row key column")
          opt[String]('c', "cf").required().action((x, config) => config.copy(cf = x)).text("column family name")
          opt[String]('m', "columns").required().action((x, config) => config.copy(columns = x)).text("columns name, eg: col1,col2")
          opt[String]('p', "namespace").optional().action((x, config) => config.copy(namespace = x)).text("hbase namespace,default: default")

      }


    }
    parser.parse(args, Config()) match {
      case Some(conf) => conf
      case None => {
        System.exit(-1)
        null
      }
    }

  }

}

