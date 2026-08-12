"""Shared bits for the sml-cicd-demo DAGs.

All environment-touching work runs in short-lived Kubernetes pods (node:20
image) in the `airflow` namespace — the Airflow image itself needs neither
node nor sml-cli. Credentials come from Kubernetes secrets:

  atscale-dev-api / atscale-qa-api / atscale-live-api   key: token
  github-token                                          key: token

Endpoints mirror config/environment.yml.
"""
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

REPO_URL = "https://github.com/muellermarco/sml-cicd-demo.git"
GITHUB_REPO = "muellermarco/sml-cicd-demo"
UPSTREAM_URL = "https://github.com/muellermarco/sml-tpcds-snowflake.git"

ATSCALE_HOSTS = {
    "dev": "https://dev.atscale-demo.com",
    "qa": "https://qa.atscale-demo.com",
    "live": "https://atscale-mm.atscale-demo.com",
}

NODE_IMAGE = "node:20-bookworm"  # includes git


def atscale_token_secret(env: str) -> Secret:
    return Secret("env", "ATSCALE_API_TOKEN", f"atscale-{env}-api", "token")


github_token_secret = Secret("env", "GITHUB_TOKEN", "github-token", "token")


def deploy_pod(task_id: str, env: str, git_ref: str,
               catalog_name: str | None = None,
               catalog_label: str | None = None) -> KubernetesPodOperator:
    """Clone the repo at git_ref and `sml-cli atscale-deploy` it to env."""
    flags = ""
    if catalog_name:
        flags += f' --catalog-name="{catalog_name}"'
    if catalog_label:
        flags += f' --catalog-label="{catalog_label}"'
    script = f"""set -euo pipefail
git clone {REPO_URL} /work && cd /work
git checkout {git_ref}
npx -y sml-cli install .
npx -y sml-cli validate . | tee /tmp/validate.log
grep -q "Validation SUCCESSFUL" /tmp/validate.log
npx -y sml-cli atscale-deploy .{flags}
"""
    return KubernetesPodOperator(
        task_id=task_id,
        name=task_id.replace("_", "-"),
        namespace="airflow",
        in_cluster=True,
        image=NODE_IMAGE,
        cmds=["bash", "-c"],
        arguments=[script],
        env_vars={
            "ATSCALE_API_URL": ATSCALE_HOSTS[env],
            # demo instances use self-signed certificates
            "NODE_TLS_REJECT_UNAUTHORIZED": "0",
        },
        secrets=[atscale_token_secret(env)],
        get_logs=True,
        is_delete_operator_pod=True,
        startup_timeout_seconds=300,
    )


def github_status(sha: str, state: str, context: str, description: str):
    """POST a commit status back to GitHub."""
    import requests
    from airflow.models import Variable

    token = Variable.get("github_token", default_var=None)
    if not token:
        print("Airflow variable 'github_token' not set — skipping status post.")
        return
    r = requests.post(
        f"https://api.github.com/repos/{GITHUB_REPO}/statuses/{sha}",
        headers={"Authorization": f"Bearer {token}",
                 "Accept": "application/vnd.github+json"},
        json={"state": state, "context": context, "description": description[:140]},
        timeout=30,
    )
    print(f"GitHub status {context}={state} for {sha}: HTTP {r.status_code}")
    r.raise_for_status()
