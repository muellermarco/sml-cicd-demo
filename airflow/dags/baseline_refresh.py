"""baseline_refresh — weekly machine PR: re-sync baseline, open drift PR.

Demo analog of a weekly converter re-run: the upstream model source
(sml-tpcds-snowflake) plays the "converter output". The DAG copies the
upstream SML content onto the `baseline` branch; if anything changed, it
commits, pushes, and opens a drift PR baseline -> main. The PR then enters
the exact same validate/review pipeline as any human change.

Needs the Kubernetes secret `github-token` (PAT with repo scope).
"""
from datetime import datetime

from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from sml_demo_common import (GITHUB_REPO_TMPL, NODE_IMAGE, UPSTREAM_URL_TMPL,
                             github_token_secret)

# Repo values come from env (GITHUB_REPO / UPSTREAM_URL), injected as
# Variable-overridable templates below — see sml_demo_common "DEFINE ONCE".
SCRIPT = """set -euo pipefail
git config --global user.name  "baseline-refresh bot"
git config --global user.email "bot@atscale-demo.com"

git clone https://x-access-token:${GITHUB_TOKEN}@github.com/${GITHUB_REPO}.git /work
cd /work
git checkout baseline

git clone --depth 1 "${UPSTREAM_URL}" /upstream
# converter output = model content only; pipeline/config files stay ours
for d in calculations connections datasets dimensions metrics models catalog.yml package.yml; do
  rm -rf "/work/$d"
  [ -e "/upstream/$d" ] && cp -R "/upstream/$d" "/work/$d" || true
done

if git diff --quiet; then
  echo "No drift — baseline is current."
  exit 0
fi

git add -A
git commit -m "baseline refresh $(date +%F): sync from upstream model source"
git push origin baseline

curl -sf -X POST "https://api.github.com/repos/${GITHUB_REPO}/pulls" \\
  -H "Authorization: Bearer ${GITHUB_TOKEN}" \\
  -H "Accept: application/vnd.github+json" \\
  -d '{"title":"Drift: baseline refresh","head":"baseline","base":"main","body":"Weekly machine PR — upstream model source changed. Review like any other change."}' \\
  || echo "PR already open or creation failed — see logs."
"""


@dag(schedule="0 5 * * 1", start_date=datetime(2026, 8, 1), catchup=False,
     tags=["sml-cicd-demo"], doc_md=__doc__)
def baseline_refresh():
    KubernetesPodOperator(
        task_id="sync_baseline_and_open_pr",
        name="baseline-refresh",
        namespace="airflow",
        in_cluster=True,
        image=NODE_IMAGE,
        cmds=["bash", "-c"],
        arguments=[SCRIPT],
        env_vars={"GITHUB_REPO": GITHUB_REPO_TMPL,
                  "UPSTREAM_URL": UPSTREAM_URL_TMPL},
        secrets=[github_token_secret],
        get_logs=True,
        is_delete_operator_pod=True,
        startup_timeout_seconds=300,
    )


baseline_refresh()
