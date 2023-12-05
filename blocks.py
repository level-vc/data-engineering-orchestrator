### .\blocks.py

from prefect.filesystems import GitHub
from prefect.filesystems import S3

##########################################
#            GH STORAGE MASTER           #
##########################################

gh = GitHub(
    repository="https://github.com/level-vc/data-engineering-orchestrator",
    reference="main",
)
gh.save("gh-main", overwrite=True)

##########################################
#              GH STORAGE DEV            #
##########################################

gh_dev = GitHub(
    repository="https://github.com/level-vc/data-engineering-orchestrator",
    reference="dev",
)
gh_dev.save("gh-dev", overwrite=True)


# ##########################################
# #              DB CRDENTIALS             #
# ##########################################

# db = DatabaseCredentials(
#     host=config["POSTGRES"]["HOST"],
#     port=config["POSTGRES"]["PORT"],
#     database=config["POSTGRES"]["DATABASE"],
#     username=config["POSTGRES"]["USER"],
#     password=config["POSTGRES"]["PASSWORD"],
#     driver="postgresql+psycopg2"
# )
# db.save("lk-rds-credentials", overwrite=True)