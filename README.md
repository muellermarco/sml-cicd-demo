# sml-cicd-demo

GitOps CI/CD for an AtScale semantic model, modeled on a production
enterprise migration pipeline. The repo **is** the deployable artifact: a
deployment is `sml-cli atscale-deploy` of a git ref to an AtScale instance.

- Model content: TPC-DS (Snowflake), with shared objects pulled from
  [sml-tpcds-common](https://github.com/muellermarco/sml-tpcds-common) via
  `package.yml`.
- Pipeline overview: [docs/CICD_PROCESS.md](docs/CICD_PROCESS.md)
- Branch model: [docs/GIT_PROCESS.md](docs/GIT_PROCESS.md)
- Environments & endpoints: [config/environment.yml](config/environment.yml)
- Orchestration: Airflow on GKE (`airflow` namespace), DAGs in
  [airflow/dags/](airflow/dags/)

## The 30-second story

1. Open a PR → GitHub Actions validates the model (no credentials needed).
2. Merge to `main` → Airflow deploys the commit to **QA** and posts the
   status back to the commit.
3. Push a `vX.Y` tag → Airflow deploys that exact ref to **Live**.
4. Nightly, Airflow checks parity QA vs Live; weekly, machine PRs arrive
   (baseline drift, usage backlog) and go through the same gate as humans.

## Local validation

```sh
npx -y sml-cli install .
npx -y sml-cli validate .
python3 tools/validate_sml.py .
```

<!-- ci smoke test 2026-08-13 -->
