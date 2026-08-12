"""sml_deploy_qa — deploy main to the QA AtScale instance on merge.

Triggered by the GitHub Actions workflow deploy-qa.yml (service-hook analog)
via POST /api/v2/dags/sml_deploy_qa/dagRuns with conf {"sha": <commit>}.
Deploys that exact commit to qa.atscale-demo.com, runs a smoke check, and
reports the result back to the commit as status context 'atscale/deploy-qa'.
"""
from datetime import datetime

from airflow.decorators import dag, task
from sml_demo_common import ATSCALE_HOSTS, deploy_pod, github_status


@dag(schedule=None, start_date=datetime(2026, 8, 1), catchup=False,
     tags=["sml-cicd-demo"], doc_md=__doc__)
def sml_deploy_qa():

    deploy = deploy_pod(
        task_id="deploy_main_to_qa",
        env="qa",
        git_ref="{{ dag_run.conf.get('sha', 'main') }}",
    )

    @task
    def smoke_check():
        """Confirm the QA instance answers after the deploy."""
        import requests
        r = requests.get(ATSCALE_HOSTS["qa"], verify=False, timeout=30)
        assert r.status_code < 500, f"QA instance unhealthy: HTTP {r.status_code}"
        print(f"QA responded with HTTP {r.status_code}")

    @task(trigger_rule="all_done")
    def report_status(**ctx):
        dag_run = ctx["dag_run"]
        sha = (dag_run.conf or {}).get("sha")
        if not sha:
            print("No sha in conf — nothing to report.")
            return
        failed = any(ti.state == "failed" for ti in dag_run.get_task_instances()
                     if ti.task_id != "report_status")
        github_status(sha,
                      "failure" if failed else "success",
                      "atscale/deploy-qa",
                      "Deploy to qa.atscale-demo.com "
                      + ("failed" if failed else "succeeded"))

    deploy >> smoke_check() >> report_status()


sml_deploy_qa()
