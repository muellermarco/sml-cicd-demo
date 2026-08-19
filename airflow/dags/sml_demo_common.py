"""Shared bits for the sml-cicd-demo DAGs.

All environment-touching work runs in short-lived Kubernetes pods (node:20
image) in the `airflow` namespace — the Airflow image itself needs neither
node nor sml-cli. Each deploy pod mints its own AtScale public-API token at
runtime from OAuth credentials, so there are no long-lived tokens to expire or
get invalidated (AtScale public tokens are single-active per user). Secrets:

  atscale-dev-oauth / atscale-qa-oauth / atscale-live-oauth
      keys: clientSecret, adminUser, adminPassword
  github-token                                          key: token

Endpoints mirror config/environment.yml.

=== Repo configuration — DEFINE ONCE ===
Point the whole pipeline at a different SML repo by setting Airflow Variables
(Admin -> Variables), no code change:
    repo_url      https://github.com/<you>/<repo>.git   (what gets deployed)
    github_repo   <you>/<repo>                           (for the GitHub API)
    upstream_url  https://github.com/<you>/<model>.git   (baseline drift source)
The DEFAULT_* values below are the fallback when a Variable is unset. (The
Airflow git-sync source that bootstraps these DAGs is separate infra — it
lives in values-airflow.yaml -> dags.gitSync.repo.)
"""
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

DEFAULT_REPO_URL = "https://github.com/muellermarco/sml-cicd-demo.git"
DEFAULT_GITHUB_REPO = "muellermarco/sml-cicd-demo"
DEFAULT_UPSTREAM_URL = "https://github.com/muellermarco/sml-tpcds-snowflake.git"

# Jinja templates — KubernetesPodOperator env_vars/arguments are rendered at
# task runtime, so the Variable override takes effect without re-parsing code.
REPO_URL_TMPL = "{{ var.value.get('repo_url', '" + DEFAULT_REPO_URL + "') }}"
GITHUB_REPO_TMPL = "{{ var.value.get('github_repo', '" + DEFAULT_GITHUB_REPO + "') }}"
UPSTREAM_URL_TMPL = "{{ var.value.get('upstream_url', '" + DEFAULT_UPSTREAM_URL + "') }}"

# Pin the deploy toolchain so a new upstream release can't silently break runs.
SML_CLI = "sml-cli@2025.12.0"


def repo_var(key: str, default: str) -> str:
    """Read a repo-config Variable at TASK runtime (not DAG-parse time)."""
    try:
        try:
            from airflow.sdk import Variable          # Airflow 3
        except ImportError:                            # pragma: no cover
            from airflow.models import Variable        # Airflow 2 fallback
        try:
            return Variable.get(key, default=default)
        except TypeError:                              # pragma: no cover
            return Variable.get(key, default_var=default)
    except Exception:                                  # noqa: BLE001
        return default

# Base hostnames (no path). sml-cli needs the `/api` base, added in the script.
# The domain comes from the Airflow Variable `atscale_domain` so one pipeline
# codebase can target any AtScale trio following the dev/qa/prod.<domain>
# convention (e.g. the GKE demo on atscale-demo.com or the AKS twin on
# atscale-se-demo.com). Hostname = https://<subdomain>.<atscale_domain>.
ATSCALE_SUBDOMAINS = {"dev": "dev", "qa": "qa", "live": "prod"}
DEFAULT_ATSCALE_DOMAIN = "atscale-demo.com"

# Jinja form for templated fields (KubernetesPodOperator arguments): resolved
# at task runtime, so DAG parsing never hits the metadata DB.
ATSCALE_DOMAIN_TMPL = (
    "{{ var.value.get('atscale_domain', '" + DEFAULT_ATSCALE_DOMAIN + "') }}"
)


def atscale_host(env: str) -> str:
    """Base URL for env, for use INSIDE task code (runtime Variable lookup)."""
    domain = repo_var("atscale_domain", DEFAULT_ATSCALE_DOMAIN)
    return f"https://{ATSCALE_SUBDOMAINS[env]}.{domain}"

NODE_IMAGE = "node:20-bookworm"  # includes git


def oauth_secrets(env: str) -> list[Secret]:
    """OAuth creds the pod uses to mint a fresh public-API token at runtime."""
    name = f"atscale-{env}-oauth"
    return [
        Secret("env", "ATSCALE_CLIENT_SECRET", name, "clientSecret"),
        Secret("env", "ATSCALE_ADMIN_USER", name, "adminUser"),
        Secret("env", "ATSCALE_ADMIN_PASSWORD", name, "adminPassword"),
    ]


github_token_secret = Secret("env", "GITHUB_TOKEN", "github-token", "token")


def deploy_pod(task_id: str, env: str, git_ref: str,
               catalog_name: str | None = None,
               catalog_label: str | None = None) -> KubernetesPodOperator:
    """Clone the repo at git_ref and `sml-cli atscale-deploy` it to env.

    Mints a short-lived public-API token in-pod: OAuth password grant ->
    /api/auth/token/public -> ATSCALE_API_TOKEN. No stored deploy token.
    """
    host = f"https://{ATSCALE_SUBDOMAINS[env]}.{ATSCALE_DOMAIN_TMPL}"
    flags = ""
    if catalog_name:
        flags += f' --catalog-name="{catalog_name}"'
    if catalog_label:
        flags += f' --catalog-label="{catalog_label}"'
    script = f"""set -euo pipefail
HOST="{host}"
OT=$(curl -sk -X POST "$HOST/auth/realms/atscale/protocol/openid-connect/token" \
  -d grant_type=password -d client_id=atscale-public-api \
  --data-urlencode client_secret="$ATSCALE_CLIENT_SECRET" \
  --data-urlencode username="$ATSCALE_ADMIN_USER" \
  --data-urlencode password="$ATSCALE_ADMIN_PASSWORD" \
  | sed -n 's/.*"access_token":"\\([^"]*\\)".*/\\1/p')
export ATSCALE_API_TOKEN=$(curl -sk -X POST -H "Authorization: Bearer $OT" \
  "$HOST/api/auth/token/public" | sed -n 's/.*"token":"\\([^"]*\\)".*/\\1/p')
test -n "$ATSCALE_API_TOKEN" || {{ echo "failed to mint public token"; exit 1; }}
export ATSCALE_API_URL="$HOST/api"
git clone "$REPO_URL" /work && cd /work
git checkout {git_ref}
npx -y {SML_CLI} install .
npx -y {SML_CLI} validate . | tee /tmp/validate.log
grep -q "Validation SUCCESSFUL" /tmp/validate.log
npx -y {SML_CLI} atscale-deploy .{flags}
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
            "NODE_TLS_REJECT_UNAUTHORIZED": "0",  # self-signed certs
            "REPO_URL": REPO_URL_TMPL,            # Variable-overridable
        },
        secrets=oauth_secrets(env),
        get_logs=True,
        is_delete_operator_pod=True,
        startup_timeout_seconds=300,
    )


def github_status(sha: str, state: str, context: str, description: str):
    """POST a commit status back to GitHub. Never fatal — a status-post hiccup
    must not fail an otherwise-successful deploy."""
    import requests
    try:
        try:
            from airflow.sdk import Variable        # Airflow 3
        except ImportError:                          # pragma: no cover
            from airflow.models import Variable      # Airflow 2 fallback
        try:
            token = Variable.get("github_token", default=None)      # Airflow 3
        except TypeError:                            # pragma: no cover
            token = Variable.get("github_token", default_var=None)  # Airflow 2
        if not token:
            print("Airflow variable 'github_token' not set — skipping status post.")
            return
        repo = repo_var("github_repo", DEFAULT_GITHUB_REPO)
        r = requests.post(
            f"https://api.github.com/repos/{repo}/statuses/{sha}",
            headers={"Authorization": f"Bearer {token}",
                     "Accept": "application/vnd.github+json"},
            json={"state": state, "context": context,
                  "description": description[:140]},
            timeout=30,
        )
        print(f"GitHub status {context}={state} for {sha}: HTTP {r.status_code}")
    except Exception as exc:  # noqa: BLE001
        print(f"GitHub status post failed (non-fatal): {exc}")
