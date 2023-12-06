import time
from prefect import flow, task
from datetime import timedelta, datetime

import boto3
import awswrangler as wr

#from lvc_engineering import athena

ENV = 'DEV'
GITHUB_BRANCH = 'main'
RUN_DATE = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

env_variables={ "ENV": ENV, "GITHUB_BRANCH": GITHUB_BRANCH }

# UTILS

def build_workflow_parameters(env, github_branch, run_date=RUN_DATE):
    workflow_parameters = []
    table_name = 'cl_organizations_blocks'
    database='entities_relation'
    if env != 'STAGING':
        table_name = table_name + '_' + env.lower()
    total_batches = wr.athena.read_sql_query(f"SELECT MAX(batch_number) as total_batches FROM {database}.{table_name}", database=database, env=env)['total_batches'][0]
    
    batches = [i for i in range(1, total_batches + 1)]
    # TODO - Add range according to max batch size
    print(batches)
    for batch in batches:
        params = {
            'EXECUTION_NAME' : 'orgs-er-batch-' + str(batch) + '-' + str(run_date).replace(':','_'),
            'ENV': env,
            'RUN_DATE': run_date,
            'GITHUB_BRANCH': github_branch,
            'BATCH_NUMBER': str(batch)
        }
        workflow_parameters.append(params)
    
    parameters_map = [
    {
        'job_name': f"er-orgs-batch-{params['BATCH_NUMBER']}",
        'job_definition': "arn:aws:batch:us-east-2:058442094236:job-definition/er-organizations-match-entities",
        'job_queue': "arn:aws:batch:us-east-2:058442094236:job-queue/etl-queue",
        **params
    }
    for params in workflow_parameters
]
    return parameters_map
# TASKS

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
    while True:
        response = batch_client.describe_jobs(jobs=[job_id])
        status = response['jobs'][0]['status']

        if status == 'SUCCEEDED':
            break
        elif status == 'FAILED':
            raise Exception(f'Job {job_id} failed')
        else:
            print(f"Job {job_id} is still in {status} status, waiting...")
            time.sleep(30) # Wait a bit before polling again


# @task
# def run_er_organizations_flow(env, github_branch, run_date):
#     print("Running ER Organizations Flow")
#     er_organizations_flow = perform_entity_resolution_on_organizations(env, github_branch)
#     er_organizations_flow.run()
    #return 'success'

# FLOWS 

# er-organizations Step Function
@flow(log_prints=True)
def perform_entity_resolution_on_organizations(env, github_branch, run_date):
    # batch_submit(
    #     job_name="er-orgs-prepare-input",
    #     job_definition="arn:aws:batch:us-east-2:058442094236:job-definition/er-organizations-prepare-input",
    #     job_queue="arn:aws:batch:us-east-2:058442094236:job-queue/etl-queue",
    #     containerOverrides={'environment': [{'name': k, 'value': v} for k, v in env_variables.items()]}
    # )

    # batch_submit(
    #     job_name="er-orgs-clean-up-data",
    #     job_definition="arn:aws:batch:us-east-2:058442094236:job-definition/er-organizations-clean-up-er-chunks",
    #     job_queue="arn:aws:batch:us-east-2:058442094236:job-queue/etl-queue",
    #     containerOverrides={'environment': [{'name': k, 'value': v} for k, v in env_variables.items()]}
    # )

    workflow_parameters = build_workflow_parameters(env, github_branch, run_date)

    # Approximate Map state with a loop (Assuming that 'Map' state runs 5 times)
    # for params in workflow_parameters:
    #     batch_submit(
    #         job_name=f"er-orgs-batch-{params['BATCH_NUMBER']}",
    #         job_definition="arn:aws:batch:us-east-2:058442094236:job-definition/er-organizations-match-entities",
    #         job_queue="arn:aws:batch:us-east-2:058442094236:job-queue/etl-queue",
    #         containerOverrides={'environment': [{'name': k, 'value': v} for k, v in params.items()]}
    #     )
    batch_submit.map(workflow_parameters)

    batch_submit(
        job_name="create-er-organizations-table",
        job_definition="arn:aws:batch:us-east-2:058442094236:job-definition/er-organizations-create-er-table",
        job_queue="arn:aws:batch:us-east-2:058442094236:job-queue/etl-queue",
        containerOverrides={'environment': [{'name': k, 'value': v} for k, v in env_variables.items()]}
    )


# perform-entity-resolution-on-organizations-and-people Step Function
@flow(log_prints=True)
def perform_entity_resolution_on_organizations_people(name: str = "world"):
    # batch_submit(
    #     job_name="copy-from-rds-to-s3",
    #     job_definition="arn:aws:batch:us-east-2:058442094236:job-definition/copy-overwrites-RDS-tables-to-Athena",
    #     job_queue="arn:aws:batch:us-east-2:058442094236:job-queue/etl-queue",
    #     containerOverrides={'environment': [{'name': k, 'value': v} for k, v in env_variables.items()]}
    # )

    perform_entity_resolution_on_organizations(ENV, GITHUB_BRANCH, RUN_DATE)

    batch_submit(
        job_name="er-people",
        job_definition="arn:aws:batch:us-east-2:058442094236:job-definition/er_people_match_entities",
        job_queue="arn:aws:batch:us-east-2:058442094236:job-queue/etl-queue",
        containerOverrides={'environment': [{'name': k, 'value': v} for k, v in env_variables.items()]}
    )

    # batch_submit(
    #     job_name="copy-from-rds-to-s3",
    #     job_definition="arn:aws:batch:us-east-2:058442094236:job-definition/copy-overwrites-RDS-tables-to-Athena",
    #     job_queue="arn:aws:batch:us-east-2:058442094236:job-queue/etl-queue",
    #     containerOverrides={'environment': [{'name': k, 'value': v} for k, v in env_variables.items()]}
    # )