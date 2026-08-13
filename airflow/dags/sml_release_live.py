"""sml_release_live — deploy a signed-off tag to the Live instance.

Triggered by the GitHub Actions workflow release-live.yml on a vX.Y tag push
(conf {"tag": ...}), or manually from the Airflow UI with a tag parameter.
Promotion is by git ref: rollback = re-run with the previous tag.
"""
from datetime import datetime

from airflow.decorators import dag
from sml_demo_common import deploy_pod


@dag(schedule=None, start_date=datetime(2026, 8, 1), catchup=False, max_active_runs=1,
     tags=["sml-cicd-demo"], doc_md=__doc__,
     params={"tag": ""})
def sml_release_live():
    deploy_pod(
        task_id="deploy_tag_to_live",
        env="live",
        git_ref="{{ dag_run.conf.get('tag') or params.tag }}",
    )


sml_release_live()
