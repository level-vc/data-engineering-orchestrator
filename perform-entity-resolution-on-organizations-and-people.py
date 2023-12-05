import time
from prefect import flow, task
from datetime import timedelta

import boto3


@task
async def batch_submit(
    job_name: str,
    job_queue: str,
    job_definition: str,
    region_name='us-east-2',
    **batch_kwargs,
) -> str:
    """
    ...
    """
    print("Preparing to submit %s job to %s job queue", job_name, job_queue)

    batch_client = boto3.client("batch", region_name=region_name)

    response = batch_client.submit_job(
        jobName=job_name,
        jobQueue=job_queue,
        jobDefinition=job_definition,
        **batch_kwargs,
    )

    job_id = response["jobId"]

    # Wait for the job to complete
    waiter = batch_client.get_waiter('job_execution_complete')
    waiter.wait(jobs=[job_id])

    return job_id

@task
def run_flow():
    print("Running ER Organizations Flow")
    #return 'success'

env = 'DEV'
github_branch = 'main'
env_variables={ "ENV": env, "GITHUB_BRANCH": github_branch }

@flow(log_prints=True)
def perform_entity_resolution_on_organizations_people(name: str = "world"):
    batch_submit(
        job_name="copy-from-rds-to-s3",
        job_definition="arn:aws:batch:us-east-2:058442094236:job-definition/copy-overwrites-RDS-tables-to-Athena",
        job_queue="arn:aws:batch:us-east-2:058442094236:job-queue/etl-queue",
        containerOverrides={'environment': [{'name': k, 'value': v} for k, v in env_variables.items()]}
    )

    run_flow()

    batch_submit(
        job_name="er-people",
        job_definition="arn:aws:batch:us-east-2:058442094236:job-definition/er_people_match_entities",
        job_queue="arn:aws:batch:us-east-2:058442094236:job-queue/etl-queue",
        containerOverrides={'environment': [{'name': k, 'value': v} for k, v in env_variables.items()]}
    )

    batch_submit(
        job_name="copy-from-rds-to-s3",
        job_definition="arn:aws:batch:us-east-2:058442094236:job-definition/copy-overwrites-RDS-tables-to-Athena",
        job_queue="arn:aws:batch:us-east-2:058442094236:job-queue/etl-queue",
        containerOverrides={'environment': [{'name': k, 'value': v} for k, v in env_variables.items()]}
    )