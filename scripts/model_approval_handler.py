import mlflow
import time
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# Get passed parameters
model_name = dbutils.widgets.get("model_name")
model_version = dbutils.widgets.get("model_version")
run_id = dbutils.widgets.get("run_id")
decision = dbutils.widgets.get("decision")

print("Approval handler received:", model_name, model_version, run_id, decision)

if not model_name or not model_version:
    raise Exception("model_name and model_version are required")

# Update approval status
spark.sql(f"""
    UPDATE mlops.model_approvals
    SET status = '{decision}'
    WHERE model_name = '{model_name}'
    AND model_version = '{model_version}'
""")

print("Approval status updated.")

# Move model to Production
client = mlflow.tracking.MlflowClient()
try:
    client.transition_model_version_stage(
        name=model_name,
        version=model_version,
        stage="Production",
        archive_existing_versions=True
    )
    print(f"Model {model_name} v{model_version} moved to PRODUCTION.")
except Exception as e:
    print("Could not transition stage:", str(e))

# Log approval event
spark.sql(f"""
    INSERT INTO mlops.model_approvals_log 
    (model_name, model_version, run_id, decision, processed_on)
    VALUES ('{model_name}', '{model_version}', '{run_id}', '{decision}', current_timestamp())
""")

print("Approval event recorded.")
