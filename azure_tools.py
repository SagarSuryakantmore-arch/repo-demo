import os
from dotenv import load_dotenv
from azure.identity import ClientSecretCredential, AzureCliCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from langchain.tools import tool

load_dotenv()

# --- AUTHENTICATION ---

# Method 1: For Student Accounts (ACTIVE)
# This uses your 'az login' credentials from the terminal.
credential = AzureCliCredential()

# Method 2: For Free Trial Accounts (Service Principal)
# Uncomment this section when you switch to a free trial account.
#
# credential = ClientSecretCredential(
#     tenant_id=os.getenv("AZURE_TENANT_ID"),
#     client_id=os.getenv("AZURE_CLIENT_ID"),
#     client_secret=os.getenv("AZURE_CLIENT_SECRET"),
# )

# Initialize Data Factory Management Client
adf_client = DataFactoryManagementClient(
    credential=credential, subscription_id=os.getenv("AZURE_SUBSCRIPTION_ID")
)

@tool
def list_pipelines(data_factory_name: str, resource_group_name: str) -> list:
    """Lists all pipelines in a given Azure Data Factory."""
    pipelines = adf_client.pipelines.list_by_factory(
        resource_group_name=resource_group_name, factory_name=data_factory_name
    )
    return [p.name for p in pipelines]

@tool
def get_pipeline_runs(
    data_factory_name: str, resource_group_name: str, pipeline_name: str
) -> list:
    """Gets the most recent runs for a specific pipeline."""
    from datetime import datetime, timedelta

    filter_params = {
        "lastUpdatedAfter": datetime.utcnow() - timedelta(days=7),
        "lastUpdatedBefore": datetime.utcnow(),
    }
    runs = adf_client.pipeline_runs.query_by_factory(
        resource_group_name=resource_group_name,
        factory_name=data_factory_name,
        filter_parameters=filter_params,
    )
    return [
        {
            "runId": run.run_id,
            "status": run.status,
            "runStart": run.run_start.isoformat(),
            "runEnd": run.run_end.isoformat() if run.run_end else "N/A",
        }
        for run in runs.value
        if run.pipeline_name == pipeline_name
    ]

@tool
def get_run_activity_logs(
    data_factory_name: str, resource_group_name: str, run_id: str
) -> list:
    """Gets the activity logs for a specific pipeline run, useful for debugging failed runs."""
    from datetime import datetime, timedelta

    filter_params = {
        "lastUpdatedAfter": datetime.utcnow() - timedelta(days=7),
        "lastUpdatedBefore": datetime.utcnow(),
    }
    activity_runs = adf_client.activity_runs.query_by_pipeline_run(
        resource_group_name=resource_group_name,
        factory_name=data_factory_name,
        run_id=run_id,
        filter_parameters=filter_params,
    )
    return [
        {
            "activityName": ar.activity_name,
            "status": ar.status,
            "error": ar.error,
        }
        for ar in activity_runs.value
    ]