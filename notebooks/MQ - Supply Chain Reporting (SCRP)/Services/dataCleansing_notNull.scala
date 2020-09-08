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

dbutils.widgets.text("scratchpad_source_path","")
val scratchpad_source_path = dbutils.widgets.get("scratchpad_source_path")
dbutils.widgets.text("scratchpad_target_path","")
val scratchpad_target_path = dbutils.widgets.get("scratchpad_target_path")

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
val pswd=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "DataOpsDB-elancoread")
val key=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "ADLS-MQ-SupplyChain")
val secret=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "Elanco-MQ-NonProd-SCRP-Secret")
val clintid=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "DataOpsServicePrincipal01-ClientID")
val tenantid=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "DataOpsServicePrincipal01-TenantID")

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

val source_path = "abfss://"+adls_container_name+"@"+adls_storage_account_name+".dfs.core.windows.net/"
val pswd=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "DataOpsDW-elancoadmin")
val key=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "ADLS-MQ-SupplyChain")
val secret=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "Elanco-MQ-NonProd-SCRP-Secret")
val clintid=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "DataOpsServicePrincipal01-ClientID")
val tenantid=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "DataOpsServicePrincipal01-TenantID")
val emp_raw = "abfss://"+adls_container_name+"@"+adls_storage_account_name+".dfs.core.windows.net/"+"Raw/"

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

// DBTITLE 1,Raw DataFrame
//spark.sql(s"refresh table ${rawtablename}")
//val rawdataframe = spark.sql(s"select * from ${rawtablename}")
//val rawdataframe= df_rawdataframe.filter(col("file_name") === raw_filter_filename) 
val rawdataframe= sqlContext.read.parquet(scratchpad_source_path)
rawdataframe.count

// COMMAND ----------

display(rawdataframe)

// COMMAND ----------

val df_DataOps_Control=df_srvc_control_table.select(
col("BusinessName")
,col("ObjectName")
,col("ColumnName")
,col("DataType")
,col("NotNullCheck")
)
.where(df_srvc_control_table.col("BusinessName").equalTo(s"$business_name") && df_srvc_control_table.col("ObjectName").equalTo(s"$object_name"))

// COMMAND ----------

df_DataOps_Control.show()

// COMMAND ----------

// DBTITLE 1,NullCheck Logic
//null_check_list
val val_metaDataList_notNullList = (df_DataOps_Control.select("ColumnName", "NotNullCheck")).where(col("BusinessName").equalTo(s"$business_name") && col("ObjectName").equalTo(s"$object_name") && col("NotNullCheck").isin("Y"))
//val metaDataList_DeDupColByList = val_metaDataList_DeDupCol.collect().map(_.getString(0)).toArray

// COMMAND ----------

val metaDataList_notNullList = val_metaDataList_notNullList.orderBy("NotNullCheck").collect.toList

// COMMAND ----------

var orderByColListBuffer = new ListBuffer[Column]()
metaDataList_notNullList.foreach {
case (colFlag) => if (colFlag(1).equals("Desc")) orderByColListBuffer += rawdataframe(colFlag(0).toString()).desc else orderByColListBuffer += rawdataframe(colFlag(0).toString())
}
val orderByCol = orderByColListBuffer.toList

// COMMAND ----------

//val orderByFirstIndex =orderByCol.dropRight(0)

// COMMAND ----------

val orderByFirstqIndex =rawdataframe.select(orderByCol: _*)

// COMMAND ----------

val withoutnull_filterCondition = orderByFirstqIndex.columns.map(x=>col(x).isNotNull).reduce(_ && _)

// COMMAND ----------

val withoutnullValue = rawdataframe.filter(withoutnull_filterCondition)

// COMMAND ----------

val withnull_filterCondition = orderByFirstqIndex.columns.map(x=>col(x).isNull).reduce(_ || _)

// COMMAND ----------

val withnullValue = rawdataframe.filter(withnull_filterCondition)

// COMMAND ----------

val cleaned_finalset =  withoutnullValue.drop("load_datetime").drop("datepart").drop("Status").drop("ServiceCode").drop("count").withColumn("ServiceCode",lit("5")).withColumn("Status",lit("Pass")).withColumn("load_datetime",lit(current_timestamp())).withColumn("datepart",lit(current_date()))

// COMMAND ----------

display(cleaned_finalset)

// COMMAND ----------

// DBTITLE 1,Insert Into Stage Table
//cleaned_finalset.write.format("parquet").mode("append").insertInto(s"${stagetablename}")
//cleaned_finalset.write.mode(SaveMode.Overwrite).format("parquet").save(s"$scratchpad_target_path")

//cleaned_finalset.write.format("parquet").mode("append").insertInto("supplychain_stage_db.5d_past_due_preq")

//cleaned_finalset.write.mode(SaveMode.Append).insertInto("supplychain_stage_db.5d_past_due_preq2")
cleaned_finalset.write.mode(SaveMode.Append).insertInto(s"${stagetablename}")

// COMMAND ----------

val rejected_finalset =  withnullValue.drop("load_datetime").drop("datepart").drop("Status").drop("ServiceCode").withColumn("ErrorMapCol",lit("NULL")).withColumn("ServiceCode",lit("3")).withColumn("Status",lit("Fail")).withColumn("load_datetime",lit(current_timestamp())).withColumn("datepart",lit(current_date()))

// COMMAND ----------

display(rejected_finalset)

// COMMAND ----------

rejected_finalset.write.format("avro").mode("append").insertInto(s"${rejectedtablename}")

// COMMAND ----------

val auditlog_df_pass = withoutnullValue.select(
   lit(s"$process_runlogId").as("ProcessRunlogId")
  ,lit(s"$business_name").as("BusinessName")
  ,lit(s"$object_name").as("ObjectName")
  ,lit("Stage").as("StorageLayer")
  ,lit("Data_Cleansing").as("ServiceCategory")
  ,lit("Data_Cleansing_NotNull").as("ServiceName")
  ,lit(s"$raw_filter_filename").as("FileName")
  ,lit("Pass").as("Status")
  ,concat(lit("Pass"),lit(s" in Data Cleansing NotNull for $object_name")).as("Description")
  ,lit(s"$column_count").as("SourceColCount")
).groupBy("ProcessRunlogId","BusinessName","ObjectName","StorageLayer","ServiceCategory","ServiceName","FileName","Status","Description","SourceColCount").count().withColumn("LoadTimestamp",lit(current_timestamp())).withColumn("Datepart",lit(current_date()))

// COMMAND ----------

val df_audittablename_pass = auditlog_df_pass.select(col("ProcessRunlogId"),
col("BusinessName"), col("ObjectName"),col("StorageLayer"),col("ServiceCategory"),col("ServiceName"),col("FileName"),col("Status"),col("Description"),col("SourceColCount"),
col("count").as("ResultRowCount"),col("LoadTimestamp"),col("Datepart")).createOrReplaceTempView("tb_audittablename_pass")

// COMMAND ----------

//auditlog_df.write.format("csv").mode("append").insertInto("dataops_service.audit_log")
spark.table("tb_audittablename_pass").write.mode(SaveMode.Append).jdbc(jdbcUrl, s"$audit_log", connectionProperties)

// COMMAND ----------

val auditlog_df_rejected = withnullValue.select(
   lit(s"$process_runlogId").as("ProcessRunlogId")
  ,lit(s"$business_name").as("BusinessName")
  ,lit(s"$object_name").as("ObjectName")
  ,lit("Stage").as("StorageLayer")
  ,lit("Data_Cleansing").as("ServiceCategory")
  ,lit("Data_Cleansing_NotNull").as("ServiceName")
  ,lit(s"$raw_filter_filename").as("FileName")
  ,lit("Fail").as("Status")
  ,concat(lit("Fail"),lit(s" in Data Cleansing NotNull for $object_name")).as("Description")
  ,lit(s"$column_count").as("SourceColCount")
).groupBy("ProcessRunlogId","BusinessName","ObjectName","StorageLayer","ServiceCategory","ServiceName","FileName","Status","Description","SourceColCount").count().withColumn("LoadTimestamp",lit(current_timestamp())).withColumn("Datepart",lit(current_date()))

// COMMAND ----------

val df_audittablename = auditlog_df_rejected.select(col("ProcessRunlogId"),
col("BusinessName"), col("ObjectName"),col("StorageLayer"),col("ServiceCategory"),col("ServiceName"),col("FileName"),col("Status"),col("Description"),col("SourceColCount"),
col("count").as("ResultRowCount"),col("LoadTimestamp"),col("Datepart")).createOrReplaceTempView("tb_audittablename_rejected")

// COMMAND ----------

//auditlog_df.write.format("csv").mode("append").insertInto("dataops_service.audit_log")
spark.table("tb_audittablename_rejected").write.mode(SaveMode.Append).jdbc(jdbcUrl, s"$audit_log", connectionProperties)

// COMMAND ----------

dbutils.notebook.exit(s"$scratchpad_target_path")