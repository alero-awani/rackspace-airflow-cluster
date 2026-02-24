"""
Credit Default Prediction Pipeline DAG

This DAG orchestrates a complete ML pipeline using S3 for intermediate storage:
1. Fetch loan data from PostgreSQL and save to S3
2. Train a Random Forest model, save model and test data to S3
3. Evaluate model performance from S3 artifacts

Runs every 10 days to retrain the model with fresh data.

EXECUTOR: KubernetesExecutor - Each task runs in its own pod on Rackspace spot instances
"""

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from kubernetes.client import models as k8s
import os

# ------------------- #
# DAG-level variables #
# ------------------- #

DAG_ID = "credit_default_pipeline"

# S3 Configuration
_AWS_CONN_ID = os.getenv("AWS_CONN_ID", "aws_default")
_S3_BUCKET = os.getenv("S3_BUCKET", "airflow-ml-artifacts")

# Postgres Configuration
_POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")
_POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", "credit_db")

# Task IDs for file path construction
_FETCH_DATA_TASK_ID = "fetch_data"
_TRAIN_MODEL_TASK_ID = "train_model"

# Default args for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,  # Increased retries for spot instance interruptions
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),  # Prevent hanging tasks
}

# KubernetesExecutor pod configuration for running on spot instances
kubernetes_pod_config = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            node_selector={
                "workload": "airflow-workers"  # Schedule on worker nodes only
            },
            restart_policy="Never",
            termination_grace_period_seconds=120,  # Graceful termination for spot interruptions
            containers=[
                k8s.V1Container(
                    name="base",
                    resources=k8s.V1ResourceRequirements(
                        requests={"cpu": "500m", "memory": "1Gi"},
                        limits={"cpu": "1000m", "memory": "2Gi"},
                    ),
                )
            ],
        )
    )
}

# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(days=10),
    catchup=False,
    default_args=default_args,
    description="Automated ML pipeline for credit default prediction on Rackspace spot instances",
    tags=["ml", "credit", "production", "spot-instances", "kubernetes", "s3"],
    doc_md=__doc__,
)
def credit_default_pipeline():
    """
    Credit default prediction ML pipeline with S3 intermediate storage.
    Each task runs in its own Kubernetes pod on Rackspace spot instances.
    """

    # ---------------- #
    # Setup Tasks      #
    # ---------------- #

    _create_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket",
        bucket_name=_S3_BUCKET,
        aws_conn_id=_AWS_CONN_ID,
    )

    # ---------------- #
    # Pipeline Tasks   #
    # ---------------- #

    @task(task_id=_FETCH_DATA_TASK_ID, executor_config=kubernetes_pod_config)
    def fetch_data(**context):
        """
        Fetch loan data from PostgreSQL and save to S3.

        Reads all loan records from the database and exports to CSV in S3.
        File location: s3://{bucket}/{dag_id}/fetch_data/{timestamp}.csv
        """
        import pandas as pd

        from datetime import datetime

        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        # Use data_interval_start which is the Airflow 3.x replacement for logical_date
        logical_date = (
            context.get("data_interval_start")
            or context.get("logical_date")
            or datetime.now()
        )
        dag_run_timestamp = logical_date.strftime("%Y%m%dT%H%M%S")

        print(f"Fetching data from PostgreSQL database: {_POSTGRES_DATABASE}")

        # Fetch from Postgres using PostgresHook
        pg_hook = PostgresHook(postgres_conn_id=_POSTGRES_CONN_ID)

        query = """
        SELECT
            loan_amount,
            income,
            credit_score,
            age,
            loan_term,
            default_status
        FROM loans
        """

        df = pg_hook.get_pandas_df(sql=query)
        print(f"Fetched {len(df)} rows from database")

        # Convert DataFrame to CSV bytes
        csv_bytes = df.to_csv(index=False).encode("utf-8")

        # Write to S3
        s3_hook = S3Hook(aws_conn_id=_AWS_CONN_ID)
        s3_key = f"{dag_id}/{task_id}/{dag_run_timestamp}.csv"

        s3_hook.load_bytes(
            bytes_data=csv_bytes, key=s3_key, bucket_name=_S3_BUCKET, replace=True
        )

        print(f"Data saved to s3://{_S3_BUCKET}/{s3_key}")
        print(f"File size: {len(csv_bytes) / 1024:.2f} KB")

    @task(task_id=_TRAIN_MODEL_TASK_ID, executor_config=kubernetes_pod_config)
    def train_model(**context):
        """
        Train Random Forest model on data from S3, save model and test data to S3.

        Reads CSV from S3, trains model, and saves:
        - Trained model: s3://{bucket}/{dag_id}/train_model/{timestamp}.pkl
        - Test data: s3://{bucket}/{dag_id}/train_model/{timestamp}_test.csv
        """
        import pandas as pd
        import joblib
        import io
        from sklearn.model_selection import train_test_split
        from sklearn.ensemble import RandomForestClassifier

        from datetime import datetime

        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        # Use data_interval_start which is the Airflow 3.x replacement for logical_date
        logical_date = (
            context.get("data_interval_start")
            or context.get("logical_date")
            or datetime.now()
        )
        dag_run_timestamp = logical_date.strftime("%Y%m%dT%H%M%S")
        upstream_task_id = _FETCH_DATA_TASK_ID

        # Read data from S3 (written by fetch_data task)
        s3_hook = S3Hook(aws_conn_id=_AWS_CONN_ID)
        data_key = f"{dag_id}/{upstream_task_id}/{dag_run_timestamp}.csv"

        print(f"Reading data from s3://{_S3_BUCKET}/{data_key}")
        csv_data = s3_hook.read_key(key=data_key, bucket_name=_S3_BUCKET)

        # Load into DataFrame
        df = pd.read_csv(io.StringIO(csv_data))
        print(f"Loaded {len(df)} rows for training")

        # Prepare features and target
        X = df.drop("default_status", axis=1)
        y = df["default_status"]

        # Train/test split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        print(f"Training set: {len(X_train)} samples")
        print(f"Test set: {len(X_test)} samples")

        # Train Random Forest model
        print("Training Random Forest classifier...")
        clf = RandomForestClassifier(
            n_estimators=100, max_depth=10, random_state=42, n_jobs=-1
        )
        clf.fit(X_train, y_train)
        print("Model training complete")

        # Save model to S3
        model_buffer = io.BytesIO()
        joblib.dump(clf, model_buffer)
        model_buffer.seek(0)

        model_key = f"{dag_id}/{task_id}/{dag_run_timestamp}.pkl"
        s3_hook.load_file_obj(
            file_obj=model_buffer, key=model_key, bucket_name=_S3_BUCKET, replace=True
        )
        print(f"Model saved to s3://{_S3_BUCKET}/{model_key}")

        # Save test data to S3
        test_df = pd.concat([X_test, y_test], axis=1)
        test_csv_bytes = test_df.to_csv(index=False).encode("utf-8")

        test_data_key = f"{dag_id}/{task_id}/{dag_run_timestamp}_test.csv"
        s3_hook.load_bytes(
            bytes_data=test_csv_bytes,
            key=test_data_key,
            bucket_name=_S3_BUCKET,
            replace=True,
        )
        print(f"Test data saved to s3://{_S3_BUCKET}/{test_data_key}")

    @task(executor_config=kubernetes_pod_config)
    def evaluate_model(**context):
        """
        Evaluate trained model from S3 and print performance metrics.

        Reads model and test data from S3, evaluates performance,
        and saves metrics to S3.
        """
        import pandas as pd
        import joblib
        import io
        from sklearn.metrics import (
            accuracy_score,
            precision_score,
            recall_score,
            f1_score,
        )

        from datetime import datetime

        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        # Use data_interval_start which is the Airflow 3.x replacement for logical_date
        logical_date = (
            context.get("data_interval_start")
            or context.get("logical_date")
            or datetime.now()
        )
        dag_run_timestamp = logical_date.strftime("%Y%m%dT%H%M%S")
        upstream_task_id = _TRAIN_MODEL_TASK_ID

        s3_hook = S3Hook(aws_conn_id=_AWS_CONN_ID)

        # Read model from S3
        model_key = f"{dag_id}/{upstream_task_id}/{dag_run_timestamp}.pkl"
        print(f"Loading model from s3://{_S3_BUCKET}/{model_key}")

        model_obj = s3_hook.get_key(key=model_key, bucket_name=_S3_BUCKET)
        model_bytes = model_obj.get()["Body"].read()
        clf = joblib.load(io.BytesIO(model_bytes))
        print("Model loaded successfully")

        # Read test data from S3
        test_data_key = f"{dag_id}/{upstream_task_id}/{dag_run_timestamp}_test.csv"
        print(f"Loading test data from s3://{_S3_BUCKET}/{test_data_key}")

        test_csv = s3_hook.read_key(key=test_data_key, bucket_name=_S3_BUCKET)
        df_test = pd.read_csv(io.StringIO(test_csv))

        # Prepare test features and labels
        X_test = df_test.drop("default_status", axis=1)
        y_test = df_test["default_status"]

        print(f"Evaluating on {len(X_test)} test samples...")

        # Predict
        y_pred = clf.predict(X_test)

        # Calculate metrics
        accuracy = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred)
        recall = recall_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred)

        metrics = (
            f"Model Performance Metrics\n"
            f"========================\n"
            f"Accuracy:  {accuracy:.4f}\n"
            f"Precision: {precision:.4f}\n"
            f"Recall:    {recall:.4f}\n"
            f"F1 Score:  {f1:.4f}\n"
        )

        print(metrics)

        # Save metrics to S3
        metrics_key = f"{dag_id}/{task_id}/{dag_run_timestamp}_metrics.txt"
        s3_hook.load_string(
            string_data=metrics, key=metrics_key, bucket_name=_S3_BUCKET, replace=True
        )
        print(f"Metrics saved to s3://{_S3_BUCKET}/{metrics_key}")

    # ---------------- #
    # Task Dependencies #
    # ---------------- #

    _fetch = fetch_data()
    _train = train_model()
    _evaluate = evaluate_model()

    # Chain: create_bucket -> fetch -> train -> evaluate
    _create_bucket >> _fetch >> _train >> _evaluate


# Instantiate the DAG
credit_default_pipeline()
