// Databricks notebook source
dbutils.widgets.text("business_name", "")
val business_name = dbutils.widgets.get("business_name")
dbutils.widgets.text("object_name", "")
val object_name = dbutils.widgets.get("object_name")
dbutils.widgets.text("landingrawpath", "")
val landingrawpath = dbutils.widgets.get("landingrawpath")
dbutils.widgets.text("control_table","")
val control_table = dbutils.widgets.get("control_table")
dbutils.widgets.text("adls_storage_account_name","")
val adls_storage_account_name = dbutils.widgets.get("adls_storage_account_name")
dbutils.widgets.text("adls_container_name","")
val adls_container_name = dbutils.widgets.get("adls_container_name") 
dbutils.widgets.text("file_format","")
val file_format = dbutils.widgets.get("file_format") 
dbutils.widgets.text("rawtablename","")
val rawtablename = dbutils.widgets.get("rawtablename")
//dbutils.widgets.text("audittablename","")
//val audittablename = dbutils.widgets.get("audittablename")
dbutils.widgets.text("filename","")
val filename = dbutils.widgets.get("filename")

dbutils.widgets.text("source_col_count","")
val source_col_count = dbutils.widgets.get("source_col_count")
dbutils.widgets.text("audit_log","")
val audit_log = dbutils.widgets.get("audit_log")

dbutils.widgets.text("process_runlogId","")
val process_runlogId = dbutils.widgets.get("process_runlogId")

// COMMAND ----------

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

// DBTITLE 1,Raw DataFrame from SourceFile
val rawdataframe= if (s"$file_format" == "csv")
spark.read.format("csv").option("header","true").option("inferSchema","true").load(s"abfss://${adls_container_name}@${adls_storage_account_name}.dfs.core.windows.net/$landingrawpath/$filename")
else
spark.read.format("avro").option("inferSchema","true").load(s"abfss://${adls_container_name}@${adls_storage_account_name}.dfs.core.windows.net/$landingrawpath/$filename")
rawdataframe.printSchema()
val rawdataframe_count = rawdataframe.count()

// COMMAND ----------

display(rawdataframe)

// COMMAND ----------

// DBTITLE 1,Control Table Read
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

// DBTITLE 1,Database Connection
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
val df = spark.read.jdbc(jdbcUrl, s"$control_table", connectionProperties)

// COMMAND ----------

val df_DataOps_Control=df.select(
col("BusinessName")
,col("ObjectName")
,col("SourceColumnName")
,col("ColSequenceNo")
,col("DataType")
)
.where(df.col("BusinessName").equalTo(s"$business_name") && df.col("ObjectName").equalTo(s"$object_name"))

// COMMAND ----------

val val_metaDataList_colVal = (df_DataOps_Control.select("SourceColumnName")).where(col("BusinessName").equalTo(s"$business_name") && col("ObjectName").equalTo(s"$object_name")).orderBy("ColSequenceNo")
val metaDataList_colVal = val_metaDataList_colVal.collect.map(_.getString(0)).toArray

// COMMAND ----------

val rawdataframe_arr = rawdataframe.columns

// COMMAND ----------

// DBTITLE 1,validate_ColumnSize
def validate_columnsize(actualDF: Array[String], expectedDF: Array[String]): String = {
  val actualDF_size= actualDF.length
  val expectedDF_size= expectedDF.length
  
if (!actualDF_size.equals(expectedDF_size)) 
  {
    return("NoOfColumnValidation "+"|"+"Fail "+"|"+$"$filename"+"|"+ "The Source File: "+$"$filename"+" #Column is " + $"$actualDF_size" + " and Target Object: "+$"$object_name" +" #Column is " + $"$expectedDF_size")
  }
        else
    {
      return("NoOfColumnValidation "+"|"+"Pass "+"|"+$"$filename"+"|"+ " #Column is same in Source File: "+$"$filename"+" and Target Object: "+$"$object_name")
    }
}

// COMMAND ----------

val result_validate_columnsize =  validate_columnsize(rawdataframe_arr,metaDataList_colVal)

// COMMAND ----------

// DBTITLE 1,validate_ColumnNames
def validate_columns(actualDF: Array[String], expectedDF: Array[String]): String = { 
    val actualDF_str = actualDF.mkString(",")
    val expectedDF_str = expectedDF.mkString(",")
    if (!actualDF_str.equals(expectedDF_str)) 
      {
        return("ColumnNameValidation "+"|"+"Fail "+"|"+$"$filename"+"|"+ "The Source File: "+$"$filename"+" Columns are " +$"$actualDF_str" + " and Target Object: "+$"$object_name" +" Columns are " + $"$expectedDF_str")
      }
        else
    {
      return("ColumnNameValidation "+"|"+"Pass "+"|"+$"$filename"+"|"+"Source File: "+$"$filename"+" and Target Object: "+$"$object_name Columns are " +$"$actualDF_str")
    }
}

// COMMAND ----------

val result_validate_columns = validate_columns(rawdataframe_arr,metaDataList_colVal)
val returns_validate_columns = (s"$result_validate_columns")
val returns_validate_columnsize = (s"$result_validate_columnsize")

// COMMAND ----------

val df_returns_validate_columns = sqlContext.sparkContext.parallelize(Seq(returns_validate_columns)).toDF().withColumn("load_txn_tm",lit(current_timestamp()))
val df_returns_validate_columnsize = sqlContext.sparkContext.parallelize(Seq(returns_validate_columnsize)).toDF().withColumn("load_txn_tm",lit(current_timestamp()))
val final_df= df_returns_validate_columns.union(df_returns_validate_columnsize)

// COMMAND ----------

val splited_final_df = final_df.withColumn("serviceName", split($"value","\\|").getItem(0)).withColumn("status", split($"value","\\|").getItem(1)).withColumn("filename", split($"value","\\|").getItem(2)).withColumn("description", split($"value", "[^a-zA-Z^\\w\\s.#,()_:-]+").getItem(3)).drop($"value")

// COMMAND ----------

splited_final_df.show(false)

// COMMAND ----------

// DBTITLE 1,Audit table Entry
val df_audittablename = splited_final_df.select(
   lit(s"$process_runlogId").as("ProcessRunlogId")
  ,lit(s"$business_name").as("BusinessName")
  ,lit(s"$object_name").as("ObjectName")
  ,lit("Raw").as("StorageLayer")
  ,lit("Data_Ingestion").as("ServiceCategory")
  ,col("serviceName").as("ServiceName")
  ,col("filename").as("FileName")
  ,col("status").as("Status")
  ,col("description").as("Description")
  ,lit(s"${source_col_count}").as("SourceColCount")
  ,lit(s"$rawdataframe_count").as("ResultRowCount")
).withColumn("LoadTimestamp",lit(current_timestamp())).withColumn("Datepart",lit(current_date())).createOrReplaceTempView("tb_audittablename")

// COMMAND ----------

spark.table("tb_audittablename").write.mode(SaveMode.Append).jdbc(jdbcUrl, s"$audit_log", connectionProperties)

// COMMAND ----------

dbutils.notebook.exit(s"Data Ingestion - Column Validation Service completed for $business_name: $object_name")