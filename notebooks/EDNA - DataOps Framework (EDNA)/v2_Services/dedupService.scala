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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import scala.util.matching.Regex
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Row
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.Column
import java.text.SimpleDateFormat

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
//val rawdataframe = spark.sql(s"select * from ${rawtablename}")

// COMMAND ----------

val rawdataframe= if (s"$file_format" == "csv")
spark.read.format("csv").option("header","true").option("inferSchema","true").load(s"abfss://${adls_container_name}@${adls_storage_account_name}.dfs.core.windows.net/$rawpath")
else
spark.read.format("avro").option("inferSchema","true").load(s"abfss://${adls_container_name}@${adls_storage_account_name}.dfs.core.windows.net/$rawpath")

// COMMAND ----------

rawdataframe.printSchema()

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
,col("DeDupCol")
,col("DeDupOrderFlag")
,col("DeDupOrderRank"))
.where(df.col("BusinessName").equalTo(s"$BusinessName") && df.col("ObjectName").equalTo(s"$ObjectName"))

// COMMAND ----------

df_DataOps_Control.show()

// COMMAND ----------

// DBTITLE 1,Dedup Logic
val val_metaDataList_DeDupCol = (df_DataOps_Control.select("ColumnName", "DeDupCol")).where(col("BusinessName").equalTo(s"$BusinessName") && col("ObjectName").equalTo(s"$ObjectName") && col("DeDupCol").isin("Y"))
val metaDataList_DeDupColByList = val_metaDataList_DeDupCol.collect().map(_.getString(0)).toArray

// COMMAND ----------

val metaDataList_DeDupOrderByList = df_DataOps_Control.select("ColumnName", "DeDupOrderFlag","DeDupOrderRank").where(col("BusinessName").equalTo(s"$BusinessName") && col("ObjectName").equalTo(s"$ObjectName")&& col("DeDupOrderFlag").isin("Asc", "Desc"))

// COMMAND ----------

val DeDupOrderByFlag_List = metaDataList_DeDupOrderByList.orderBy("DeDupOrderRank").drop("DeDupOrderRank").collect.toList

// COMMAND ----------

var orderByColListBuffer = new ListBuffer[Column]()
DeDupOrderByFlag_List.foreach {
case (colFlag) => if (colFlag(1).equals("Desc")) orderByColListBuffer += rawdataframe(colFlag(0).toString()).desc else orderByColListBuffer += rawdataframe(colFlag(0).toString())
}
val orderByCol = orderByColListBuffer.toList

// COMMAND ----------

val orderByLastIndex = orderByCol.reverse.head
val orderByColwithoutLastIndex = orderByCol.dropRight(0)

// COMMAND ----------

val window = Window.partitionBy(metaDataList_DeDupColByList.head, metaDataList_DeDupColByList.tail: _*).orderBy(orderByCol: _*)
val dfwithoutDuplicate_r1 = rawdataframe.withColumn("rank", rank().over(window)).where(col("rank") === 1)//.drop("rank")
val dfDuplicate_mdr1 = rawdataframe.withColumn("rank", rank().over(window)).where(col("rank") > 1)//.drop("rank")
val dfwithoutDuplicate = dfwithoutDuplicate_r1.withColumn("rank", row_number().over(window)).where(col("rank") === 1)//.drop("rank")
val dfDuplicate = dfwithoutDuplicate_r1.withColumn("rank", row_number().over(window)).where(col("rank") > 1)//.drop("rank")

// COMMAND ----------

dfwithoutDuplicate_r1.show()

// COMMAND ----------

dfwithoutDuplicate.show() //Final frame

// COMMAND ----------

dfDuplicate.show()

// COMMAND ----------

dfDuplicate_mdr1.show()

// COMMAND ----------

dbutils.notebook.exit(s"Dedup Service completed for $BusinessName: $ObjectName")