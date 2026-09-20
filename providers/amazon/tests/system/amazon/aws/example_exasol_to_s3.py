# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from datetime import datetime

from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.transfers.exasol_to_s3 import ExasolToS3Operator
from airflow.providers.common.compat.sdk import DAG, chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.exasol.hooks.exasol import exasol_fetch_all_handler

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.amazon.aws.utils import SystemTestContextBuilder

sys_test_context_task = SystemTestContextBuilder().build()

DAG_ID = "example_exasol_to_s3"

# This example needs an Exasol connection. Exasol Community Edition is enough to run it;
# see https://github.com/exasol/docker-db for a local instance.
EXASOL_CONN_ID = "exasol_default"
EXASOL_TABLE = "AIRFLOW_EXASOL_TO_S3_EXAMPLE"

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args={"conn_id": EXASOL_CONN_ID, "handler": exasol_fetch_all_handler},
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]

    s3_bucket = f"{env_id}-exasol-to-s3-bucket"
    s3_key = f"{env_id}-exasol-to-s3-key.csv"

    create_s3_bucket = S3CreateBucketOperator(task_id="create_s3_bucket", bucket_name=s3_bucket)

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        sql=f"""
            CREATE OR REPLACE TABLE {EXASOL_TABLE} (
                a VARCHAR(100),
                b DECIMAL(18,0)
            );
        """,
    )

    insert_data = SQLExecuteQueryOperator(
        task_id="insert_data",
        sql=f"""
            INSERT INTO {EXASOL_TABLE} (a, b)
            VALUES
              ('a', 1),
              ('b', 2),
              ('c', 3);
        """,
    )

    # [START howto_transfer_exasol_to_s3]
    exasol_to_s3 = ExasolToS3Operator(
        task_id="exasol_to_s3",
        query_or_table=f"SELECT * FROM {EXASOL_TABLE}",
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True,
        exasol_conn_id=EXASOL_CONN_ID,
    )
    # [END howto_transfer_exasol_to_s3]

    drop_table = SQLExecuteQueryOperator(
        task_id="drop_table",
        sql=f"DROP TABLE IF EXISTS {EXASOL_TABLE};",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_s3_bucket = S3DeleteBucketOperator(
        task_id="delete_s3_bucket",
        bucket_name=s3_bucket,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        # TEST SETUP
        test_context,
        create_s3_bucket,
        create_table,
        insert_data,
        # TEST BODY
        exasol_to_s3,
        # TEST TEARDOWN
        drop_table,
        delete_s3_bucket,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
