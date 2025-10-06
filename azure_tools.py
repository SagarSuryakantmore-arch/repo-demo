# azure_tools.py

import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory.models import PipelineResource
from azure.core.exceptions import ResourceNotFoundError
from langchain_core.tools import tool

load_dotenv()

credential = ClientSecretCredential(
    tenant_id=os.getenv("AZURE_TENANT_ID"),
    client_id=os.getenv("AZURE_CLIENT_ID"),
    client_secret=os.getenv("AZURE_CLIENT_SECRET"),
)

subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
adf_client = DataFactoryManagementClient(credential=credential, subscription_id=subscription_id)
resource_client = ResourceManagementClient(credential=credential, subscription_id=subscription_id)


@tool
def list_all_data_factories_in_subscription() -> list:
    """
    Lists all Data Factories in the entire subscription.
    Returns a list of dictionaries, each containing the factory's name and its resource group.
    """
    try:
        factories = adf_client.factories.list()
        factory_details = []
        for factory in factories:
            rg_name = "Unknown"
            try:
                parts = factory.id.split("/")
                rg_index = [part.lower() for part in parts].index("resourcegroups")
                rg_name = parts[rg_index + 1]
            except (ValueError, IndexError) as e:
                print(f"Could not parse resource group from ID: {factory.id}. Error: {e}")

            factory_details.append({
                "factory_name": factory.name,
                "resource_group": rg_name,
            })
        return factory_details
    except Exception as e:
        return [{"error": f"Error listing all data factories: {e}"}]


@tool
def list_pipelines(resource_group_name: str, data_factory_name: str) -> list:
    """Lists all pipelines in a given Azure Data Factory."""
    try:
        pipelines = adf_client.pipelines.list_by_factory(
            resource_group_name=resource_group_name, factory_name=data_factory_name
        )
        return [p.name for p in pipelines]
    except Exception as e:
        return [f"Error listing pipelines: {e}"]


@tool
def get_pipeline_runs(
    resource_group_name: str, data_factory_name: str, days: int, pipeline_name: str = None
) -> list:
    """
    Gets pipeline runs from the last N days.
    If a pipeline_name is provided, it filters for that specific pipeline's runs.
    """
    try:
        filter_params = {
            "lastUpdatedAfter": datetime.utcnow() - timedelta(days=days),
            "lastUpdatedBefore": datetime.utcnow(),
        }
        runs = adf_client.pipeline_runs.query_by_factory(
            resource_group_name=resource_group_name,
            factory_name=data_factory_name,
            filter_parameters=filter_params,
        )
        run_details = [
            {
                "pipelineName": run.pipeline_name,
                "runId": run.run_id,
                "status": run.status,
                "runStart": run.run_start.isoformat(),
                "runEnd": run.run_end.isoformat() if run.run_end else "In Progress",
                "durationInMs": run.duration_in_ms,
                "message": run.message,
            }
            for run in runs.value
        ]
        if pipeline_name:
            return [run for run in run_details if run['pipelineName'] == pipeline_name]
        return run_details
    except Exception as e:
        return [f"Error getting pipeline runs: {e}"]


@tool
def get_run_activity_logs(
    resource_group_name: str, data_factory_name: str, run_id: str
) -> list:
    """Gets the activity logs for a specific pipeline run, useful for debugging failed runs."""
    try:
        filter_params = {
            "lastUpdatedAfter": datetime.utcnow() - timedelta(days=60),
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
                "input": ar.input,
                "output": ar.output,
            }
            for ar in activity_runs.value
        ]
    except Exception as e:
        return [f"Error getting activity logs: {e}"]


@tool
def get_pipeline_definition(resource_group_name: str, data_factory_name: str, pipeline_name: str) -> dict:
    """
    Retrieves the full JSON definition of a specific pipeline.
    This is needed to understand the pipeline's structure before attempting a fix.
    """
    try:
        pipeline = adf_client.pipelines.get(
            resource_group_name=resource_group_name,
            factory_name=data_factory_name,
            pipeline_name=pipeline_name,
        )
        return pipeline.as_dict()
    except ResourceNotFoundError:
        return {"error": f"Pipeline '{pipeline_name}' not found."}
    except Exception as e:
        return {"error": f"Error getting pipeline definition: {e}"}


@tool
def update_pipeline(resource_group_name: str, data_factory_name: str, pipeline_name: str, pipeline_definition: dict) -> dict:
    """
    Updates an existing pipeline with a new definition using the create_or_update operation.
    The `pipeline_definition` must be a valid pipeline structure.
    """
    try:
        pipeline_resource = PipelineResource(
            activities=pipeline_definition.get('activities'),
            parameters=pipeline_definition.get('parameters'),
            variables=pipeline_definition.get('variables'),
            annotations=pipeline_definition.get('annotations'),
        )
        updated_pipeline = adf_client.pipelines.create_or_update(
            resource_group_name=resource_group_name,
            factory_name=data_factory_name,
            pipeline_name=pipeline_name,
            pipeline=pipeline_resource
        )
        return {"status": "Success", "pipeline_name": updated_pipeline.name}
    except Exception as e:
        return {"error": f"Error updating pipeline: {e}"}


@tool
def create_pipeline_run(resource_group_name: str, data_factory_name: str, pipeline_name: str) -> dict:
    """
    Creates a new run for a specified pipeline and returns the run ID.
    """
    try:
        run_response = adf_client.pipelines.create_run(
            resource_group_name=resource_group_name,
            factory_name=data_factory_name,
            pipeline_name=pipeline_name,
        )
        return {"runId": run_response.run_id}
    except Exception as e:
        return {"error": f"Error creating pipeline run: {e}"}


@tool
def get_pipeline_run(resource_group_name: str, data_factory_name: str, run_id: str) -> dict:
    """
    Retrieves the details of a specific pipeline run, including its status.
    """
    try:
        run = adf_client.pipeline_runs.get(
            resource_group_name=resource_group_name,
            factory_name=data_factory_name,
            run_id=run_id,
        )
        return {
            "runId": run.run_id,
            "status": run.status,
            "message": run.message,
        }
    except Exception as e:
        return {"error": f"Error getting pipeline run status: {e}"}