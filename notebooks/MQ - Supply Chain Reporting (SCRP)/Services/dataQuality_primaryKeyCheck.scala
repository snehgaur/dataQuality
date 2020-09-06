// Databricks notebook source
dbutils.widgets.text("business_name", "")
val business_name = dbutils.widgets.get("business_name")
dbutils.widgets.text("object_name", "")
val object_name = dbutils.widgets.get("object_name")
dbutils.widgets.text("control_table","")
val control_table = dbutils.widgets.get("control_table")
dbutils.widgets.text("adls_storage_account_name","")
val adls_storage_account_name = dbutils.widgets.get("adls_storage_account_name")
dbutils.widgets.text("adls_container_name","")
val adls_container_name = dbutils.widgets.get("adls_container_name") 
dbutils.widgets.text("rawtablename","")
val rawtablename = dbutils.widgets.get("rawtablename")
dbutils.widgets.text("stagetablename","")
val stagetablename = dbutils.widgets.get("stagetablename")
dbutils.widgets.text("rejectedtablename","")
val rejectedtablename = dbutils.widgets.get("rejectedtablename")

dbutils.widgets.text("audit_log","")
val audit_log = dbutils.widgets.get("audit_log")
dbutils.widgets.text("src_control_table", "")
val src_control_table = dbutils.widgets.get("src_control_table")
dbutils.widgets.text("srvc_control_table", "")
val srvc_control_table = dbutils.widgets.get("srvc_control_table")
dbutils.widgets.text("process_runlog", "")
val process_runlog = dbutils.widgets.get("process_runlog")

dbutils.widgets.text("raw_filter","")
val raw_filter = dbutils.widgets.get("raw_filter")
dbutils.widgets.text("raw_filter_filename","")
val raw_filter_filename = dbutils.widgets.get("raw_filter_filename")
dbutils.widgets.text("column_count","")
val column_count = dbutils.widgets.get("column_count")

dbutils.widgets.text("scratchpad_dqdt_obj_path","")
val scratchpad_dqdt_obj_path = dbutils.widgets.get("scratchpad_dqdt_obj_path")
dbutils.widgets.text("scratchpad_dqpk_obj_path","")
val scratchpad_dqpk_obj_path = dbutils.widgets.get("scratchpad_dqpk_obj_path")

dbutils.widgets.text("process_runlogId","")
val process_runlogId = dbutils.widgets.get("process_runlogId")

// COMMAND ----------

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.expressions.Window
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.Column
import java.text.SimpleDateFormat
import org.apache.spark.sql.{ SQLContext, DataFrame }
import org.apache.spark.sql.Row
import scala.collection.mutable.MultiMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf
import org.apache.hadoop.fs._
import org.apache.spark.sql.functions.lit
import scala.collection.Seq
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf;
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql._
import scala.collection.JavaConversions._
import java.lang.String
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.spark.sql.DataFrameReader
import scala.reflect.io.Path
import java.io.File
import org.apache.spark.sql.types.StructType
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.DateTime
import org.apache.spark.sql.types.IntegerType
import scala.util.matching.Regex
import java.util.regex.Pattern
import scala.collection.immutable.Map
import collection.mutable.{ HashMap, MultiMap, Set }
import java.io.PrintWriter
import java.io.StringWriter
import org.apache.spark.sql.functions._

// COMMAND ----------

// DBTITLE 1,ADLS Configuration
val source_path = "abfss://"+adls_container_name+"@"+adls_storage_account_name+".dfs.core.windows.net/"
val pswd=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "DataOpsDW-elancoadmin")
val key=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "ADLS-MQ-SupplyChain")
val secret=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "Elanco-MQ-NonProd-SCRP-Secret")
val clintid=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "DataOpsServicePrincipal01-ClientID")
val tenantid=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "DataOpsServicePrincipal01-TenantID")
val scratchpad_path_obj = "abfss://"+adls_container_name+"@"+adls_storage_account_name+".dfs.core.windows.net/"+"Raw/Sratchpad/"+s"$object_name"+s"/dqpk_$object_name"

spark.conf.set("fs.azure.account.key.bdaze1imqdl01.blob.core.windows.net",key)
spark.conf.set("fs.azure.account.auth.type."+adls_storage_account_name+".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type."+adls_storage_account_name+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."+adls_storage_account_name+".dfs.core.windows.net", clintid)
spark.conf.set("fs.azure.account.oauth2.client.secret."+adls_storage_account_name+".dfs.core.windows.net",secret)
//"Elanco-DataOps-NonProd-EDNA-Secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+adls_storage_account_name+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenantid+"/oauth2/token")

spark.sparkContext.hadoopConfiguration.set("fs.azure.account.auth.type."+adls_storage_account_name+".dfs.core.windows.net", "OAuth")
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth.provider.type."+adls_storage_account_name+".dfs.core.windows.net",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.id."+adls_storage_account_name+".dfs.core.windows.net", clintid)
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.secret."+adls_storage_account_name+".dfs.core.windows.net", secret)
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.endpoint."+adls_storage_account_name+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenantid+"/oauth2/token")

// COMMAND ----------

// DBTITLE 1,Raw DataFrame
//spark.sql(s"refresh table ${rawtablename}")
//val df_rawdataframe = spark.sql(s"select * from ${rawtablename}")
//val rawdataframe= df_rawdataframe.filter(col("file_name") === raw_filter_filename) 
val rawdataframe= sqlContext.read.parquet(scratchpad_dqdt_obj_path)
rawdataframe.count

// COMMAND ----------

display(rawdataframe)//.show()

// COMMAND ----------

// DBTITLE 1,JDBC Connection
val jdbcUsername = dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "DataOpsDB-username")
val jdbcPassword = dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "DataOpsDB-elancoread")
Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
val jdbcHostname = "dataops-dev.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "diqframework"
 
// Create the JDBC URL without passing in the user and password parameters.
val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
// Create a Properties() object to hold the parameters.
import java.util.Properties
val connectionProperties = new Properties()
 
connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")
 
val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
connectionProperties.setProperty("Driver", driverClass)
val df_src_control_table = spark.read.jdbc(jdbcUrl, s"$src_control_table", connectionProperties)
val df_process_runlog = spark.read.jdbc(jdbcUrl, s"$process_runlog", connectionProperties)
val df_audit_log = spark.read.jdbc(jdbcUrl, s"$audit_log", connectionProperties)
val df_srvc_control_table = spark.read.jdbc(jdbcUrl, s"$srvc_control_table", connectionProperties)

// COMMAND ----------

val df_DataOps_Control=df_srvc_control_table.select(
col("BusinessName")
,col("ObjectName")
,col("ColumnName")
,col("DQPrimaryKey")
).where(df_srvc_control_table.col("BusinessName").equalTo(s"$business_name") && df_srvc_control_table.col("ObjectName").equalTo(s"$object_name"))

// COMMAND ----------

df_DataOps_Control.show()

// COMMAND ----------

// DBTITLE 1,Primary Key Logic
val val_metaDataList_pKCol = (df_DataOps_Control.select("ColumnName", "DQPrimaryKey")).where(col("BusinessName").equalTo(s"$business_name") && col("ObjectName").equalTo(s"$object_name") && col("DQPrimaryKey").isin("Y"))
val metaDataList_pKCol = val_metaDataList_pKCol.collect().map(_.getString(0)).toArray

// COMMAND ----------

val data_counts =rawdataframe.groupBy(metaDataList_pKCol.head, metaDataList_pKCol.tail: _*).count().alias("counts")

// COMMAND ----------

val window = Window.partitionBy(metaDataList_pKCol.head, metaDataList_pKCol.tail: _*).orderBy(metaDataList_pKCol.head, metaDataList_pKCol.tail: _*)

// COMMAND ----------

val result_pk = rawdataframe.select('*, count('*) over window as "count").filter("count==1")

// COMMAND ----------

val result_pk_rejected = rawdataframe.select('*, count('*) over window as "count").filter("count>1")

// COMMAND ----------

val rejected_finalset =  result_pk_rejected.withColumn("ErrorMapCol",col("count")).drop("count").drop("load_datetime").drop("datepart").drop("ServiceCode").drop("Status").withColumn("ServiceCode",lit("2")).withColumn("Status",lit("Fail")).withColumn("load_datetime",lit(current_timestamp())).withColumn("datepart",lit(current_date()))

// COMMAND ----------

display(rejected_finalset) //drop("ErrorMapCol")

// COMMAND ----------

val cleaned_finalset =  result_pk.drop("load_datetime").drop("datepart").drop("Status").drop("ServiceCode").drop("count").withColumn("ServiceCode",lit("2")).withColumn("Status",lit("Pass")).withColumn("load_datetime",lit(current_timestamp())).withColumn("datepart",lit(current_date()))

// COMMAND ----------

display(cleaned_finalset)

// COMMAND ----------

//cleaned_finalset.write.format("avro").mode("append").insertInto(s"${stagetablename}")
cleaned_finalset.write.mode(SaveMode.Overwrite).format("parquet").save(s"$scratchpad_dqpk_obj_path")

// COMMAND ----------

rejected_finalset.write.format("avro").mode("append").insertInto(s"${rejectedtablename}")

// COMMAND ----------

val df_audittablename_val_pass = cleaned_finalset.select(
   lit(s"$process_runlogId").as("ProcessRunlogId")
  ,lit(s"$business_name").as("BusinessName")
  ,lit(s"$object_name").as("ObjectName")
  ,lit("Stage").as("StorageLayer")
  ,lit("Data_Quality").as("ServiceCategory")
  ,lit("Data_Quality_PrimaryKey").as("ServiceName")
  ,lit(s"$raw_filter_filename").as("FileName")
  ,col("status").as("Status")
  ,concat(col("status"),lit(s" in Data Quality Primary Key check for $object_name")).as("Description")
  ,lit(s"$column_count").as("SourceColCount")
).groupBy("ProcessRunlogId","BusinessName","ObjectName","StorageLayer","ServiceCategory","ServiceName","FileName","Status","Description","SourceColCount").count().withColumn("LoadTimestamp",lit(current_timestamp())).withColumn("Datepart",lit(current_date()))

// COMMAND ----------

val df_audittablename_pass = df_audittablename_val_pass.select(col("ProcessRunlogId"),
col("BusinessName"), col("ObjectName"),col("StorageLayer"),col("ServiceCategory"),col("ServiceName"),col("FileName"),col("Status"),col("Description"),col("SourceColCount"),
col("count").as("ResultRowCount"),col("LoadTimestamp"),col("Datepart"))//.createOrReplaceTempView("tb_audittablename_pass")

// COMMAND ----------

df_audittablename_pass.show()

// COMMAND ----------

val df_audittablename_val_rejected = rejected_finalset.select(
  lit(s"$process_runlogId").as("ProcessRunlogId")
  ,lit(s"$business_name").as("BusinessName")
  ,lit(s"$object_name").as("ObjectName")
  ,lit("Stage").as("StorageLayer")
  ,lit("Data_Quality").as("ServiceCategory")
  ,lit("Data_Quality_PrimaryKey").as("ServiceName")
  ,lit(s"$raw_filter_filename").as("FileName")
  ,col("status").as("Status")
  ,concat(col("status"),lit(s" in Data Quality Primary Key check for $object_name")).as("Description")
  ,lit(s"$column_count").as("SourceColCount")
).groupBy("ProcessRunlogId","BusinessName","ObjectName","StorageLayer","ServiceCategory","ServiceName","FileName","Status","Description","SourceColCount").count().withColumn("LoadTimestamp",lit(current_timestamp())).withColumn("Datepart",lit(current_date()))

// COMMAND ----------

val df_audittablename_rejected = df_audittablename_val_rejected.select(col("ProcessRunlogId"),
col("BusinessName"), col("ObjectName"),col("StorageLayer"),col("ServiceCategory"),col("ServiceName"),col("FileName"),col("Status"),col("Description"),col("SourceColCount"),
col("count").as("ResultRowCount"),col("LoadTimestamp"),col("Datepart"))//.createOrReplaceTempView("tb_audittablename_rejected")

// COMMAND ----------

val auditlog_df = df_audittablename_pass.union(df_audittablename_rejected).createOrReplaceTempView("tb_audittablename")

// COMMAND ----------

//auditlog_df.write.format("csv").mode("append").insertInto("dataops_service.audit_log")
spark.table("tb_audittablename").write.mode(SaveMode.Append).jdbc(jdbcUrl, s"$audit_log", connectionProperties)

// COMMAND ----------

dbutils.notebook.exit(s"$scratchpad_dqpk_obj_path")
//dbutils.notebook.exit(s"Data Quality Service for PK Check is completed for $business_name: $object_name")