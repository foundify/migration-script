# Databricks notebook source
# MAGIC %md #Define API Connection Details

# COMMAND ----------

databricksURL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
myToken = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

dbutils.widgets.removeAll()

dbutils.widgets.text("cluster_id", "", "cluster_id")

# COMMAND ----------

# MAGIC %md #Import List of Tables to Migrate

# COMMAND ----------

schema_df = spark.sql("""select distinct(schema) from nigel_list""")
schema_list = schema_df.select('schema').rdd.map(lambda row : row[0]).collect()
print(schema_list)

# COMMAND ----------

# Create a dictionary containing each schema as a key with the value being a list of tables in that schema
import pprint
notebook_dict = {}

for x in schema_list:
  df = spark.sql(f"""select * from nigel_list where Schema = '{x}'""")
  table_list = df.select('Table').rdd.map(lambda row : row[0]).collect()
  notebook_dict[x] = table_list
  
pprint.pprint(notebook_dict)

# COMMAND ----------

# MAGIC %md #Create a multi-task job using the Jobs API 2.1 (in progress)

# COMMAND ----------

#Creates the initial job and saves the Job ID to pass to subsequent API calls

import requests
import json
import pprint

cluster_id = dbutils.widgets.get('cluster_id')

payload = {
"name": "DB2 Migration Job",
"tasks": [
  {
    "task_key": "DB2_Migration",
    "notebook_task": {
        "notebook_path": "/Repos/nigel.silva@lovelytics.com/migration-script/top_level_task",
        "source": "WORKSPACE"
      },
    "existing_cluster_id": cluster_id
  }  
],
"format": "MULTI_TASK"
}

header = {'Authorization': 'Bearer {}'.format(myToken)}
endpoint = '/api/2.1/jobs/create'

resp = requests.post(
  databricksURL + endpoint,
  data=json.dumps(payload),
  headers=header
)

job = resp.json()
print(job)
job_id = job['job_id']
print(job_id)

# COMMAND ----------

# Create all tasks

import requests
import json
import pprint


for x in notebook_dict:
  payload = {
  "job_id": job_id,
  "new_settings": 
      {
      "tasks": [
        {
          "task_key": x,
          "depends_on": [
            {
            "task_key": "DB2_Migration"
            }
          ],
          "notebook_task": {
              "notebook_path": "/Repos/nigel.silva@lovelytics.com/migration-script/top_level_task",
              "source": "WORKSPACE",
              "base_parameters": {
                "SCHEMA": x
                }
            },
          "existing_cluster_id": cluster_id
        }  
      ]
    }
  }
  
  json_data = json.dumps(payload)
  

  header = {'Authorization': 'Bearer {}'.format(myToken)}
  endpoint = '/api/2.1/jobs/update'

  resp = requests.post(
    databricksURL + endpoint,
    data=json_data,
    headers=header
  )
  
  if resp.json() == {}:
    print(f'Created top level task: {x}')
  
    for i in notebook_dict[x]:
      payload = {
      "job_id": job_id,
      "new_settings": 
          {
          "tasks": [
            {
              "task_key": f"{x}_{i}",
              "depends_on": [
                  {
                  "task_key": x
                  }
                ],
              "notebook_task": {
                  "notebook_path": "/Repos/nigel.silva@lovelytics.com/migration-script/jdbc_query",
                  "source": "WORKSPACE",
                  "base_parameters": {
                    "SCHEMA": x,
                    "TABLE": i
                    }
                },
              "existing_cluster_id": cluster_id
            }  
          ]
        }
      }

      json_data = json.dumps(payload)


      header = {'Authorization': 'Bearer {}'.format(myToken)}
      endpoint = '/api/2.1/jobs/update'

      resp = requests.post(
        databricksURL + endpoint,
        data=json_data,
        headers=header
      )

      if resp.json() == {}:
        print(f'Created: {x}/{i}')

# COMMAND ----------


