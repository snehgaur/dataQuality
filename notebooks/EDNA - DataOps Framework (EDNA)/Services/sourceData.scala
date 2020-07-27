// Databricks notebook source
val BusinessName = "DataOps"
val ObjectName = "EmpDetail_3"
val control_table = "MQSupplyChain.DataOps_Control"
val rawpath = "/dataops_controlframework/control_files/DedupSampleData2.csv"
val adls_storage_account_name= "bdaze1imqdl01"
val adls_container_name = "mq-supplychain"
val file_format = "csv"
val rawtablename = "dataops_service.emp_details"

// COMMAND ----------

// DBTITLE 1,Dedup Service Call
val result = dbutils.notebook.run("/EDNA - DataOps Framework (EDNA)/Services/dedupService/", 60, Map("BusinessName" -> s"$BusinessName", "ObjectName" -> s"$ObjectName", "control_table" -> s"$control_table", "adls_storage_account_name" -> s"$adls_storage_account_name", "adls_container_name" -> s"$adls_container_name", "rawpath" -> s"$rawpath", "file_format" -> s"$file_format", "rawtablename" -> s"$rawtablename"))