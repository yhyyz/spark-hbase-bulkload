package com.aws.analytics.bak

import com.aws.analytics.conf.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableOutputFormat}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer


/**
 * backup, do not use
 */
object HBaseBulkLoad {
 private val log = LoggerFactory.getLogger("spark-hbase-bulkload")
 def main(args: Array[String]): Unit = {
  Logger.getLogger("org").setLevel(Level.WARN)
  val params = Config.parseConfig(HBaseBulkLoad, args)
  val conf = new SparkConf()
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.enable", "true")
    .set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  // 配置 HBase
  val hadoopConf = new Configuration()
  val hbaseConf = HBaseConfiguration.create(hadoopConf)

  hbaseConf.set("hbase.zookeeper.quorum", params.hbaseZK)
  hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, params.namespace+":"+params.tableName)
  val hbaseConn =ConnectionFactory.createConnection(hbaseConf)
   log.info("create hbase conn:"+ hbaseConn.toString)
  val job = Job.getInstance(hbaseConf)
  job.setOutputKeyClass(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable])
  job.setOutputValueClass(classOf[org.apache.hadoop.hbase.client.Result])
  job.setOutputFormatClass(classOf[org.apache.hadoop.hbase.mapreduce.TableOutputFormat[org.apache.hadoop.hbase.io.ImmutableBytesWritable]])
   log.info("read source file: "+ params.sourceDir)
  val colList = params.columns.split(",")
  val selectList = (params.rowKey+","+params.columns).split(",")
   val data = spark.read.json(params.sourceDir)
     .select(selectList.map(col): _*)
     .rdd.map(row=> {

    val family = params.cf
    val kvList = ListBuffer[KeyValue]()
    val rowKey = row.getAs[String](params.rowKey).trim
     //val value = row.getAs[String](params.rowValue).trim
//    val listBuffer = ListBuffer[String]()
   val colListSort = colList.sortWith(_ < _)
    for (col <- colListSort) {
     //listBuffer += row.getAs[String](col).trim
     val colValue = row.getAs[String](col).trim
     val kv = new KeyValue(rowKey.getBytes, family.getBytes, col.getBytes, colValue.getBytes)
     kvList += kv
    }
    //val colValueList = listBuffer.toList
    //val colValueList = for (col <- colList) yield  row.getAs[String](col).trim


    //(rowKey,colValueList)
    (new org.apache.hadoop.hbase.io.ImmutableBytesWritable(rowKey.getBytes),kvList)
   })
//     .sortByKey()
//     .map{
//   case (rowKey, colValueList) =>
//    //val put = new Put(rowKey.getBytes)
//    val family = params.cf
//    val qualifier = params.columns
//    val kvList = ListBuffer[KeyValue]()
//    //put.addColumn(family.getBytes, qualifier.getBytes, value.getBytes)
//    for ((colValue, index) <- colValueList.zipWithIndex) {
//     val colName = colList(index)
//     val kv = new KeyValue(rowKey.getBytes, family.getBytes, colName.getBytes, colValue.getBytes)
//     kvList += kv
//    }
//    //val kv = new KeyValue(rowKey.getBytes, family.getBytes, qualifier.getBytes, value.getBytes)
//    (new org.apache.hadoop.hbase.io.ImmutableBytesWritable, kvList)
//  }
     .flatMapValues(s => {
   s.iterator
  }).sortBy(x => x._1, true)

  val tableName=TableName.valueOf(params.namespace,params.tableName)
   HFileOutputFormat2.configureIncrementalLoadMap(job,hbaseConn.getTable(tableName).getDescriptor)

   data.saveAsNewAPIHadoopFile(params.targetDir, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration)
   log.info("save hfile success: "+ params.targetDir)
   spark.stop()
 }

}
