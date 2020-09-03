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
val pswd=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "DataOpsDW-elancoadmin")
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

// DBTITLE 1,Raw DataFrame
spark.sql(s"refresh table ${rawtablename}")
val df_rawdataframe = spark.sql(s"select * from ${rawtablename}")
val rawdataframe= df_rawdataframe.filter(col("file_name") === raw_filter_filename) 

// COMMAND ----------

display(rawdataframe)

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
,col("DQDataTypeCategory")
,col("DQNotNullCheckFlag")
,col("DQDateFormat")
,col("DQPatternMatch"))
.where(df_srvc_control_table.col("BusinessName").equalTo(s"$business_name") && df_srvc_control_table.col("ObjectName").equalTo(s"$object_name"))

// COMMAND ----------

df_DataOps_Control.show()

// COMMAND ----------

// DBTITLE 1,Validate Fractional like Double & Integral like Integer
def validateIntFracBooln(columnName: String, check: String): String = {
			check match {
			case ("integral") => {
				var err_msg = "valid"
                        val intChkPattern ="-?\\d+?\\d*"
						try {
							if ((columnName != ("") && !"null".equalsIgnoreCase(columnName.trim())) && (!columnName.trim().matches(intChkPattern))) {
								err_msg = columnName + " Is Not Valid"
							}
						} catch {
						case t: Throwable =>
						t.printStackTrace();
						err_msg = "Exception during Integer Check"
						}
				return err_msg

			}
			case ("fractional") => {
				var err_msg = "valid"
						val doubleChkPattern = "[-+]?[0-9]+(\\.){0,1}[0-9]*"
						try {
							if ((columnName != ("") && !"null".equalsIgnoreCase(columnName.trim())) && (!columnName.trim().matches(doubleChkPattern))) {
								err_msg = columnName + " Is Not Valid"
							}
						} catch {
						case t: Throwable =>
						t.printStackTrace();
						err_msg = "Exception during Double Check"
						}
				return err_msg

			}
           case ("boolean") => {
				var err_msg = "valid"
						val doubleChkPattern = "^(true|false|TRUE|FALSE|True|False|1|0)$"
						try {
							if ((columnName != ("") && !"null".equalsIgnoreCase(columnName.trim())) && (!columnName.trim().matches(doubleChkPattern))) {
								err_msg = columnName + " Is Not Valid"
							}
						} catch {
						case t: Throwable =>
						t.printStackTrace();
						err_msg = "Exception during Boolean Check"
						}
				return err_msg

			}

			}

	}

// COMMAND ----------

//validateIntDouble("True ","boolean")

// COMMAND ----------

// DBTITLE 1,validationDateTime
def validationDateTime(col: String, pattern: String): String =
			try {
				java.time.LocalDateTime.parse(col.trim(), java.time.format.DateTimeFormatter.ofPattern(pattern))
				var trueMessage = "valid"
				return (trueMessage)
			} 
	catch {case e : Exception => 
	val sw = new StringWriter
	e.printStackTrace(new PrintWriter(sw))

	var falseMessage = "Invalid"
	return (falseMessage)
	}

// COMMAND ----------

// DBTITLE 1,validationDate
def validationDate(col: String, pattern: String): String =
			try {
				java.time.LocalDate.parse(col.trim(), java.time.format.DateTimeFormatter.ofPattern(pattern))
				var trueMessage = "valid"
				return (trueMessage)
			} 
	catch {case e : Exception => 
	val sw = new StringWriter
	e.printStackTrace(new PrintWriter(sw))

	var falseMessage = "Invalid"
	return (falseMessage)
	}

// COMMAND ----------

//validationDate("9/19/2017","M/dd/yyyy")

// COMMAND ----------

// DBTITLE 1,valregexCheck - For String Check
import scala.util.matching.Regex
def valregexCheck(columnName: String, check: String): String = {
				var err_msg = "valid"
						try {
							if ((columnName != ("") && !"null".equalsIgnoreCase(columnName.trim())) && (!columnName.trim().matches(check))) {
								err_msg = columnName + " Is Not Valid"
							}
						} catch {
						case t: Throwable =>
						t.printStackTrace();
						err_msg = "Exception during Integer Check"
						}
				return err_msg
			}  

// COMMAND ----------

// DBTITLE 1,Error Map Function
def errmapFunc(errMap: Map[String, String], key: String, value: String, flag: String): Map[String, String] = {
			def errColumnList = errMap.getOrElse(key, "")
					val newValue = if (errColumnList.size == 0) value else errColumnList + "|" + value
					var errMapNew: Map[String, String] = Map.empty[String, String]
							flag match {
							case "R" => {
										return errMapNew + ("Status" -> "Pass")
							}
							case "P" => {
                                        return errMapNew + ("Status" -> "Fail") + (key -> newValue)
							}
					}
	}

// COMMAND ----------

// DBTITLE 1,DataQuality RowWise
def dataQualityRowWiseFn(row: Row, mm: MultiMap[String, Row]): Row = {

			var errMap = Map("Status" -> "Pass")
					mm.keySet.foreach(
                      		x => if (x == "vNNF") {

								mm(x).foreach(y => if ((row.getAs(y(0).toString().trim()) == null) || "null".equalsIgnoreCase(row.getAs(y(0).toString().trim())))

									errMap = errmapFunc(errMap, "vNNF", y(0).toString(), y(1).toString()))

							} //Not Null / vNNF /P for Check / R to ignore/NotNullFunction
                            else if (x == "vTSF") {

                                    mm(x).foreach(y => if (validationDateTime(row.getAs(y(0).toString()),y(1).toString()) != "valid")

									errMap = errmapFunc(errMap, "vTSF", y(0).toString(), "P"))

							} //validationDateTime/vTSF/TimeStampFunction
                            else if (x == "vDTF") {

                                    mm(x).foreach(y => if (validateIntFracBooln(row.getAs(y(0).toString()), y(1).toString()) != "valid")

                                     errMap = errmapFunc(errMap, "vDTF", y(0).toString(), "P")
                                             )

							}//validateIntDouble/vDTF/DataTypeFunction
                      
                                else if (x == "vDFF") { 

                                          mm(x).foreach(y => if (validationDate(row.getAs(y(0).toString()), y(1).toString()) == "Invalid")

                                          errMap = errmapFunc(errMap, "vDFF", y(0).toString(), "P")
                                                  )
                            }//validationDate/vDFF/DateFormatFunction
                      
                                else if (x == "vSRF") { 

                                          mm(x).foreach(y => if (valregexCheck(row.getAs(y(0).toString()), y(1).toString()) != "valid")

                                          errMap = errmapFunc(errMap, "vSRF", y(0).toString(), "P")
                                                  )
                            }//validationString/vSRF/StringRegexFunction
                      
                    )
					var errString = errMap.toString()
					Row.merge(row, Row(errString))
	}

// COMMAND ----------

// DBTITLE 1,Creating Metadata List: Control Table 
val val_metaDataList_IntFractBoolnCol = (df_DataOps_Control.select("ColumnName", "DQDataTypeCategory")).where(col("BusinessName").equalTo(s"$business_name") && col("ObjectName").equalTo(s"$object_name")&& (col("DQDataTypeCategory").equalTo("integral")||col("DQDataTypeCategory").equalTo("fractional")||col("DQDataTypeCategory").equalTo("boolean")))
val metaDataList_IntFractBoolnCol = val_metaDataList_IntFractBoolnCol.collect

// COMMAND ----------

val val_metaDataList_dateCol = (df_DataOps_Control.select("ColumnName", "DQDateFormat")).where(col("BusinessName").equalTo(s"$business_name") && col("ObjectName").equalTo(s"$object_name")&& col("DQDataTypeCategory").equalTo("date"))
val metaDataList_dateCol = val_metaDataList_dateCol.collect

// COMMAND ----------

val val_metaDataList_dateTimestampCol = (df_DataOps_Control.select("ColumnName", "DQDateFormat")).where(col("BusinessName").equalTo(s"$business_name") && col("ObjectName").equalTo(s"$object_name")&& col("DQDataTypeCategory").equalTo("timestamp"))
val metaDataList_dateTimestampCol = val_metaDataList_dateTimestampCol.collect

// COMMAND ----------

val val_metaDataList_notNullCol = (df_DataOps_Control.select("ColumnName", "DQNotNullCheckFlag")).where(col("BusinessName").equalTo(s"$business_name") && col("ObjectName").equalTo(s"$object_name")&& col("DQNotNullCheckFlag").isin("R", "P"))//P for Check & R to ignore
val metaDataList_notNullCol = val_metaDataList_notNullCol.collect

// COMMAND ----------

val val_metaDataList_regex = (df_DataOps_Control.select("ColumnName", "DQPatternMatch")).where(col("BusinessName").equalTo(s"$business_name") && col("ObjectName").equalTo(s"$object_name")&& col("DQDataTypeCategory").equalTo("string")&& !col("DQPatternMatch").isin("null"))
val metaDataList_regex = val_metaDataList_regex.collect

// COMMAND ----------

// DBTITLE 1,Processing for DQ Checks
val dqCheckMultiMap = new HashMap[String, Set[Row]] with MultiMap[String, Row]
metaDataList_dateTimestampCol.foreach(y => dqCheckMultiMap.addBinding("vTSF", y))
metaDataList_regex.foreach(y => dqCheckMultiMap.addBinding("vSRF", y))
metaDataList_IntFractBoolnCol.foreach(y => dqCheckMultiMap.addBinding("vDTF", y))
metaDataList_dateCol.foreach(y => dqCheckMultiMap.addBinding("vDFF", y))
metaDataList_notNullCol.foreach(y => dqCheckMultiMap.addBinding("vNNF", y))

// COMMAND ----------

//val dqCheckMultiMap = new HashMap[String, Set[Row]] with MultiMap[String, Row]
//metaDataList_dateTimestampCol.foreach(y => dqCheckMultiMap.addBinding("vTSF", y))
//metaDataList_regex.foreach(y => dqCheckMultiMap.addBinding("vSRF", y))
//metaDataList_IntFractBoolnCol.foreach(y => dqCheckMultiMap.addBinding("vDTF", y))
//metaDataList_dateCol.foreach(y => dqCheckMultiMap.addBinding("vDFF", y))
//metaDataList_notNullCol.foreach(y => dqCheckMultiMap.addBinding("vNNF", y))

// COMMAND ----------

val currSchema = rawdataframe.schema
val currSchemaWithErrCol = currSchema.add("err", StringType)//.add("err_all", StringType)//Adding Temporary Error column

// COMMAND ----------

val rddWithError = rawdataframe.rdd.map(row => dataQualityRowWiseFn(row, dqCheckMultiMap))//RDD operation to perform Row wise operation to perform DQ

// COMMAND ----------

val dfWithErrCol = spark.createDataFrame(rddWithError, currSchemaWithErrCol)//Creation of Dataframe

// COMMAND ----------

dfWithErrCol.show(false)

// COMMAND ----------

    def String_to_Map(errString: String): Map[String , String] = {
    val test = errString.substring(4, errString.length - 1).split(",").map(_.split("->")).map { case Array(k, v) => (k.substring(0, k.length - 1).trim(), v.substring(1, v.length).trim()) }.toMap
    //println("Map String is "+test)
       return test
  }

// COMMAND ----------

val Udf = udf(String_to_Map _)//UDF for converting Temporary Error column from String Type to Map Type

// COMMAND ----------

val dfWithMapErrCol = dfWithErrCol.withColumn("ErrorMapCol", Udf(dfWithErrCol { "err" })).drop("err")

// COMMAND ----------

//dfWithMapErrCol.show(false)

// COMMAND ----------

val mapToTupleUDF = udf((sku: Map[String, String]) => if(sku != null) sku.toSeq(0) else null)

val finalset= dfWithMapErrCol.withColumn("ErrorMapCol_Tuple", mapToTupleUDF($"ErrorMapCol")).withColumn("Status", when($"ErrorMapCol_Tuple".isNotNull, $"ErrorMapCol_Tuple._2")).drop("ErrorMapCol_Tuple")

// COMMAND ----------

val rejected_finalset =  finalset.drop("load_datetime").drop("datepart").withColumn("load_datetime",lit(current_timestamp())).withColumn("datepart",lit(current_date())).filter('Status like "Fail")

// COMMAND ----------

val cleaned_finalset =  finalset.drop("load_datetime").drop("datepart").withColumn("load_datetime",lit(current_timestamp())).withColumn("datepart",lit(current_date())).filter('Status like "Pass")

// COMMAND ----------

cleaned_finalset.write.format("avro").mode("append").insertInto(s"${stagetablename}")

// COMMAND ----------

rejected_finalset.write.format("avro").mode("append").insertInto(s"${rejectedtablename}")

// COMMAND ----------

val df_audittablename_val = finalset.select(
   lit(s"$business_name").as("BusinessName")
  ,lit(s"$object_name").as("ObjectName")
  ,lit("Raw").as("StorageLayer")
  ,lit("Data_Quality").as("ServiceCategory")
  ,lit("Data_Quality_Datatype").as("ServiceName")
  ,lit(s"$raw_filter_filename").as("FileName")
  ,col("status").as("Status")
  ,concat(col("status"),lit(s" in Data Quality Datatype for $object_name")).as("Description")
  ,lit("NULL").as("SourceColCount")
).groupBy("BusinessName","ObjectName","StorageLayer","ServiceCategory","ServiceName","FileName","Status","Description","SourceColCount").count().withColumn("LoadTimestamp",lit(current_timestamp())).withColumn("Datepart",lit(current_date()))

// COMMAND ----------

val df_audittablename = df_audittablename_val.select(
col("BusinessName"), col("ObjectName"),col("StorageLayer"),col("ServiceCategory"),col("ServiceName"),col("FileName"),col("Status"),col("Description"),col("SourceColCount"),
col("count").as("ResultRowCount"),col("LoadTimestamp"),col("Datepart")).createOrReplaceTempView("tb_audittablename")

// COMMAND ----------

/*val auditlog_df = finalset.select(
 lit(s"$business_name").as("business_name")
,lit(s"$object_name").as("object_name")
,lit("dataQuality").as("service_name")
,lit("stage").as("storage_layer")
,col("status").as("status")  
,concat(col("status"),lit(" in Data Quality Check")).as("description")
).groupBy("business_name","object_name","service_name","storage_layer","status","description").count().as("result_count").
withColumn("load_datetime",lit(current_timestamp())).withColumn("datepart",lit(current_date()))
*/

// COMMAND ----------

//auditlog_df.write.format("csv").mode("append").insertInto("dataops_service.audit_log")
spark.table("tb_audittablename").write.mode(SaveMode.Append).jdbc(jdbcUrl, s"$audit_log", connectionProperties)

// COMMAND ----------

dbutils.notebook.exit(s"Data Quality Service is completed for $business_name: $object_name")