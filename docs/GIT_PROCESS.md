# Git Process — Branch Model

## Branches

| Branch | Owner | Purpose |
|---|---|---|
| `main` | humans (via PR) | The deployable truth. Always validates cleanly; every commit auto-deploys to QA. |
| `baseline` | machine (`baseline_refresh` DAG) | Mirror of the upstream model source (`config/environment.yml -> upstream.model_source`). Never edited by hand; drift arrives as a PR into `main`. |
| `feature/*` | developers | One change per branch; PR into `main`. Deployable to a personal sandbox catalog on Development via the `sml_deploy_dev` DAG. |
| `usage-backlog-*` | machine (`usage_trace_ingest` DAG) | Weekly refresh of `docs/USAGE_BACKLOG.md` as a reviewable PR. |

## Rules

1. **Nothing reaches `main` without a PR** — including machine changes.
   The PR gate (`validate.yml`) runs `sml-cli validate` plus the
   cross-reference integrity check; both must pass.
2. **Merges deploy.** A merge to `main` triggers the QA deploy through the
   GitHub Action → Airflow REST hook. If you are not ready to land on QA,
   keep the PR open.
3. **Releases are tags.** Pushing `vX.Y` is the sign-off action; the tag —
   and only the tag — is what Live runs. Rollback = re-release the previous
   tag.
4. **`baseline` is read-only for humans.** If the upstream conversion is
   wrong, fix the upstream source; the next drift PR carries the correction
   through review like everything else.

## Answering "what is deployed?"

- QA: the latest commit on `main` (`git log -1 main`).
- Live: the latest `v*` tag (`git describe --tags --abbrev=0`).
