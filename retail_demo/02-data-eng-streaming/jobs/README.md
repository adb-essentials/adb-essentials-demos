# Workflows job config

```json
{
    "settings": {
        "name": "ADB TPCH Workflow ",
        "email_notifications": {
            "no_alert_for_skipped_runs": false
        },
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "create_database",
                "notebook_task": {
                    "notebook_path": "/Repos/tahir.fayyaz@databricks.com/adb-essentials-retail-demo/03-data-engineering/jobs/create_database",
                    "base_parameters": {
                        "database": "adb_tpch_10_4_photon"
                    }
                },
                "existing_cluster_id": "0414-085951-iybtghtf",
                "timeout_seconds": 0,
                "email_notifications": {},
                "description": ""
            },
            {
                "task_key": "source_to_staging_dims",
                "depends_on": [
                    {
                        "task_key": "create_database"
                    }
                ],
                "notebook_task": {
                    "notebook_path": "/Repos/tahir.fayyaz@databricks.com/adb-essentials-retail-demo/03-data-engineering/jobs/source_to_staging_dimension_tables",
                    "base_parameters": {
                        "database": "adb_tpch_10_4_photon"
                    }
                },
                "existing_cluster_id": "0414-085951-iybtghtf",
                "timeout_seconds": 0,
                "email_notifications": {},
                "description": ""
            },
            {
                "task_key": "source_to_staging_facts",
                "depends_on": [
                    {
                        "task_key": "create_database"
                    }
                ],
                "notebook_task": {
                    "notebook_path": "/Repos/tahir.fayyaz@databricks.com/adb-essentials-retail-demo/03-data-engineering/jobs/source_to_staging_fact_tables",
                    "base_parameters": {
                        "database": "adb_tpch_10_4_photon"
                    }
                },
                "existing_cluster_id": "0414-085951-iybtghtf",
                "timeout_seconds": 0,
                "email_notifications": {},
                "description": ""
            },
            {
                "task_key": "staging_to_processed_facts",
                "depends_on": [
                    {
                        "task_key": "source_to_staging_dims"
                    },
                    {
                        "task_key": "source_to_staging_facts"
                    }
                ],
                "notebook_task": {
                    "notebook_path": "/Repos/tahir.fayyaz@databricks.com/adb-essentials-retail-demo/03-data-engineering/jobs/staging_to_processed_facts",
                    "base_parameters": {
                        "database": "adb_tpch_10_4_photon"
                    }
                },
                "existing_cluster_id": "0414-085951-iybtghtf",
                "timeout_seconds": 0,
                "email_notifications": {},
                "description": ""
            },
            {
                "task_key": "processed_to_aggregates",
                "depends_on": [
                    {
                        "task_key": "staging_to_processed_facts"
                    }
                ],
                "notebook_task": {
                    "notebook_path": "/Repos/tahir.fayyaz@databricks.com/adb-essentials-retail-demo/03-data-engineering/jobs/processed_to_aggregates",
                    "base_parameters": {
                        "database": "adb_tpch_10_4_photon"
                    }
                },
                "existing_cluster_id": "0414-085951-iybtghtf",
                "timeout_seconds": 0,
                "email_notifications": {},
                "description": ""
            }
        ],
        "format": "MULTI_TASK"
    }
}
```