import time
import os
os.environ['AWS_REGION'] = 'us-east-2'
from prefect import flow, task
from prefect_aws.batch import batch_submit
from datetime import timedelta

import boto3

@task
def batch_submit_job(job_name: str, job_queue: str, job_definition: str, env: dict):
    #batch = boto3.client('batch', region_name='us-east-2')
    job_id = batch_submit(
        job_name=job_name,
        job_definition=job_definition,
        job_queue=job_queue,
        containerOverrides={'environment': [{'name': k, 'value': v} for k, v in env.items()]}
    )
    # response = batch.submit_job(
    #     jobName=job_name,
    #     jobQueue=job_queue,
    #     jobDefinition=job_definition,
    #     containerOverrides={'environment': [{'name': k, 'value': v} for k, v in env.items()]}
    # )
    
    # Get JobId from response
    #job_id = response['jobId']

    # Poll job status
    # while True:
    #     response = batch.describe_jobs(jobs=[job_id])
    #     status = response['jobs'][0]['status']

    #     if status == 'SUCCEEDED':
    #         break
    #     elif status == 'FAILED':
    #         raise Exception(f'Job {job_id} failed')
    #     else:
    #         print(f"Job {job_id} is still in {status} status, waiting...")
    #         time.sleep(120) # Wait a bit before polling again

@task
def run_flow():
    print("Running ER Organizations Flow")
    #return 'success'

env = 'DEV'
github_branch = 'main'

@flow(log_prints=True)
def perform_entity_resolution_on_organizations_people(name: str = "world"):
    batch_submit_job(
        job_name="copy-from-rds-to-s3",
        job_definition="arn:aws:batch:us-east-2:058442094236:job-definition/copy-overwrites-RDS-tables-to-Athena",
        job_queue="arn:aws:batch:us-east-2:058442094236:job-queue/etl-queue",
        env={ "ENV": env, "GITHUB_BRANCH": github_branch }
    )

    run_flow()

    batch_submit_job(
        job_name="er-people",
        job_definition="arn:aws:batch:us-east-2:058442094236:job-definition/er_people_match_entities",
        job_queue="arn:aws:batch:us-east-2:058442094236:job-queue/etl-queue",
        env={ "ENV": env, "GITHUB_BRANCH": github_branch }
    )

    batch_submit_job(
        job_name="copy-from-rds-to-s3",
        job_definition="arn:aws:batch:us-east-2:058442094236:job-definition/copy-overwrites-RDS-tables-to-Athena",
        job_queue="arn:aws:batch:us-east-2:058442094236:job-queue/etl-queue",
        env={ "ENV": env, "GITHUB_BRANCH": github_branch }
    )