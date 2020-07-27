// Databricks notebook source
dbutils.widgets.text("BusinessName", "")
val BusinessName = dbutils.widgets.get("BusinessName")
dbutils.widgets.text("ObjectName", "")
val ObjectName = dbutils.widgets.get("ObjectName")
dbutils.widgets.text("rawpath", "")
val rawpath = dbutils.widgets.get("rawpath")
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

// COMMAND ----------

import org.apache.spark.sql.{ SQLContext, DataFrame }
import org.apache.spark.sql.SparkSession
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormatter
import java.io.PrintWriter
import scala.util.matching.Regex
import org.apache.spark.sql.Row
import java.io.StringWriter
import scala.collection.mutable.MultiMap
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.hadoop.fs._
import org.apache.spark.sql.functions.lit
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import scala.collection.Seq
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf;
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Dataset
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.io.File
import scala.collection.JavaConversions._
import java.lang.String
import java.io.File
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

// COMMAND ----------

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
spark.sql(s"refresh table ${rawtablename}")
val rawdataframe = spark.sql(s"select * from ${rawtablename}")

// COMMAND ----------

rawdataframe.printSchema()

// COMMAND ----------

val rawdataframe_ingestion= if (s"$file_format" == "csv")
spark.read.format("csv").option("header","true").option("inferSchema","true").load(s"abfss://${adls_container_name}@${adls_storage_account_name}.dfs.core.windows.net/$rawpath")
else
spark.read.format("avro").option("inferSchema","true").load(s"abfss://${adls_container_name}@${adls_storage_account_name}.dfs.core.windows.net/$rawpath")

// COMMAND ----------

rawdataframe_ingestion.printSchema()

// COMMAND ----------

rawdataframe.show()

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

val df: DataFrame = spark.read
  .format("com.databricks.spark.sqldw")
  .option("url", "jdbc:sqlserver://bdaze1isqdwdb01.database.windows.net:1433;database=BDAZE1ISQDWSV01;user=elancoadmin;password="+pswd+";encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;")
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("tempDir", "wasbs://"+adls_container_name+"@"+adls_storage_account_name+".blob.core.windows.net/tempDirs")
  .option("query", s"select * from $control_table")
  .load

// COMMAND ----------

val df_DataOps_Control=df.select(
col("BusinessName")
,col("ObjectName")
,col("ColumnName")
,col("col_SequenceNo")
,col("DataType")
)
.where(df.col("BusinessName").equalTo(s"$BusinessName") && df.col("ObjectName").equalTo(s"$ObjectName"))

// COMMAND ----------

df_DataOps_Control.show()

// COMMAND ----------

val val_metaDataList_colVal = (df_DataOps_Control.select("ColumnName")).where(col("BusinessName").equalTo(s"$BusinessName") && col("ObjectName").equalTo(s"$ObjectName")).orderBy("col_SequenceNo")
val metaDataList_colVal = val_metaDataList_colVal.collect.map(_.getString(0)).toArray

// COMMAND ----------

val rawdataframe_ingestion_arr = rawdataframe.columns
//val rawdataframe_ingestion_arr = rawdataframe_ingestion.columns

// COMMAND ----------

// DBTITLE 1,validate_ColumnSize
def validate_columnsize(actualDF: Array[String], expectedDF: Array[String]): String = {
  val actualDF_size= actualDF.length
  val expectedDF_size= expectedDF.length
  
if (!actualDF_size.equals(expectedDF_size)) 
  {
    return("ColumnSizeValidation "+"|"+"Invalid "+"|"+"The Source Column Size is " + $"$actualDF_size" + " and Target Column Size is " + $"$expectedDF_size")
  }
        else
    {
      return("ColumnSizeValidation "+"|"+"Valid "+"|"+"Source & Target")
    }
}

// COMMAND ----------

val result_validate_columnsize =  validate_columnsize(metaDataList_colVal,rawdataframe_ingestion_arr)

// COMMAND ----------

// DBTITLE 1,validate_ColumnNames
def validate_columns(actualDF: Array[String], expectedDF: Array[String]): String = { 
    val actualDF_str = actualDF.mkString(",")
    val expectedDF_str = expectedDF.mkString(",")
    if (!actualDF_str.equals(expectedDF_str)) 
      {
        return("ColumnValidation "+"|"+"Invalid "+"|"+"The Source Columns are " +$"$actualDF_str" + " and Target Columns are " + $"$expectedDF_str")
      }
        else
    {
      return("ColumnValidation "+"|"+"Valid "+"|"+"Source and Target Columns are " +$"$actualDF_str")
    }
}

// COMMAND ----------

val result_validate_columns = validate_columns(rawdataframe_ingestion_arr,metaDataList_colVal)

// COMMAND ----------

val returns_validate_columns = (s"$result_validate_columns")
val returns_validate_columnsize = (s"$result_validate_columnsize")

// COMMAND ----------

val df_returns_validate_columns = sqlContext.sparkContext.parallelize(Seq(returns_validate_columns)).toDF().withColumn("load_txn_tm",lit(current_timestamp()))
val df_returns_validate_columnsize = sqlContext.sparkContext.parallelize(Seq(returns_validate_columnsize)).toDF().withColumn("load_txn_tm",lit(current_timestamp()))

// COMMAND ----------

val final_df= df_returns_validate_columns.union(df_returns_validate_columnsize)

// COMMAND ----------

final_df.show(false)

// COMMAND ----------

val splited_final_df = final_df.withColumn("serviceName", split($"value","\\|").getItem(0)).withColumn("status", split($"value","\\|").getItem(1)).withColumn("description", split($"value", "[^a-zA-Z^\\w\\s,]+").getItem(2)).drop($"value")

// COMMAND ----------

splited_final_df.show(false)

// COMMAND ----------

splited_final_df.write.format("csv").mode("append").insertInto("dataops_service.logs")

// COMMAND ----------

// MAGIC %sql
// MAGIC refresh table dataops_service.logs;
// MAGIC select * from dataops_service.logs