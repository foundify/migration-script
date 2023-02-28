# Databricks notebook source
# Clear all widgets to create a blank canvas
dbutils.widgets.removeAll()


# Define widgets for job parameters
dbutils.widgets.text("SCHEMA", "", "Schema")
dbutils.widgets.text("TABLE", "", "Table")

# COMMAND ----------

try:
  db2_table = (spark.read
    .format("jdbc")
    .option("url", "<jdbc_url>")
    .option("dbtable", "<table_name>")
    .option("user", "<username>")
    .option("password", "<password>")
    .load()
  )

  db2_table.write.mode("overwrite").format("delta").saveAsTable(f'{dbutils.widgets.get("SCHEMA")}.{dbutils.widgets.get("TABLE")}')
except: 
  print(f'{dbutils.widgets.get("SCHEMA")}.{dbutils.widgets.get("TABLE")}')

# COMMAND ----------


