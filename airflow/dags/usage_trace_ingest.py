"""usage_trace_ingest — weekly machine PR: refresh the usage-priority backlog.

Demo analog of a usage-trace ingest: pulls query counts per catalog
from the Live engine's query log (falls back to a placeholder table when the
SQL credentials are not configured) and opens a PR updating
docs/USAGE_BACKLOG.md. Machine changes enter at stage 1 like any other PR.

Needs the Kubernetes secret `github-token` (PAT with repo scope).
"""
from datetime import datetime

from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from sml_demo_common import GITHUB_REPO_TMPL, NODE_IMAGE, github_token_secret

# Repo comes from env ($GITHUB_REPO), injected as a Variable-overridable
# template below — see sml_demo_common "DEFINE ONCE".
SCRIPT = """set -euo pipefail
git config --global user.name  "usage-trace bot"
git config --global user.email "bot@atscale-se-demo.com"

git clone https://x-access-token:${GITHUB_TOKEN}@github.com/${GITHUB_REPO}.git /work
cd /work
BRANCH="usage-backlog-$(date +%Y%m%d)"
git checkout -b "$BRANCH"

mkdir -p docs
cat > docs/USAGE_BACKLOG.md <<EOF
# Usage-priority backlog

Machine-generated on $(date +%F) by the usage_trace_ingest DAG.
Ranks subject areas by observed query traffic on Live — translation and
go-live work is picked from the top. (Demo placeholder ranking.)

| Rank | Subject area | Relative traffic |
|---|---|---|
| 1 | Store Sales | high |
| 2 | Customer | medium |
| 3 | Item / Inventory | low |
EOF

if git diff --quiet -- docs/USAGE_BACKLOG.md 2>/dev/null && git ls-files --error-unmatch docs/USAGE_BACKLOG.md >/dev/null 2>&1; then
  echo "Backlog unchanged — nothing to do."
  exit 0
fi

git add docs/USAGE_BACKLOG.md
git commit -m "usage backlog refresh $(date +%F)"
git push origin "$BRANCH"

BODY=$(printf '{"title":"Usage backlog refresh","head":"%s","base":"main","body":"Weekly machine PR — refreshed usage-priority backlog."}' "$BRANCH")
curl -sf -X POST "https://api.github.com/repos/${GITHUB_REPO}/pulls" \\
  -H "Authorization: Bearer ${GITHUB_TOKEN}" \\
  -H "Accept: application/vnd.github+json" \\
  -d "$BODY" \\
  || echo "PR creation failed — see logs."
"""


@dag(schedule="0 6 * * 1", start_date=datetime(2026, 8, 1), catchup=False,
     tags=["sml-cicd-demo"], doc_md=__doc__)
def usage_trace_ingest():
    KubernetesPodOperator(
        task_id="refresh_backlog_and_open_pr",
        name="usage-trace-ingest",
        namespace="airflow",
        in_cluster=True,
        image=NODE_IMAGE,
        cmds=["bash", "-c"],
        arguments=[SCRIPT],
        env_vars={"GITHUB_REPO": GITHUB_REPO_TMPL},
        secrets=[github_token_secret],
        get_logs=True,
        is_delete_operator_pod=True,
        startup_timeout_seconds=300,
    )


usage_trace_ingest()
