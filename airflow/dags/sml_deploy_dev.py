"""sml_deploy_dev — deploy a feature branch to the Development instance.

Trigger manually (UI or REST) with conf {"branch": "<feature-branch>"}.
Each branch lands as its own catalog (TPCDS-<branch>) so developers can
work side by side on dev.atscale-demo.com — the 'developer sandbox' stage.
"""
from datetime import datetime

from airflow.decorators import dag
from sml_demo_common import deploy_pod


@dag(schedule=None, start_date=datetime(2026, 8, 1), catchup=False,
     tags=["sml-cicd-demo"], doc_md=__doc__,
     params={"branch": "main"})
def sml_deploy_dev():
    deploy_pod(
        task_id="deploy_branch_to_dev",
        env="dev",
        git_ref="{{ params.branch }}",
        catalog_name="TPCDS-{{ params.branch | replace('/', '-') }}",
        catalog_label="TPC-DS ({{ params.branch }})",
    )


sml_deploy_dev()
