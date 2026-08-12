"""sml_regression_nightly — nightly parity check, QA vs Live.

Demo analog of a nightly legacy-parity run: execute a set of canned
SQL queries against both the QA and Live engines (AtScale Postgres wire
protocol, in-cluster service atscale-engine-sql.<ns>.svc:15432) and compare
the results. A mismatch fails the run and reopens the related work items.

Needs Airflow variables `atscale_sql_user` / `atscale_sql_password`
(an AtScale login valid on both instances). Skips gracefully if unset.
"""
from datetime import datetime

from airflow.decorators import dag, task

CATALOG = "TPCDS-MM-Snowflake"
QUERIES = [
    'SELECT COUNT(*) FROM information_schema.tables',
]
ENGINES = {
    "qa": "atscale-engine-sql.atscale-qa.svc.cluster.local",
    "live": "atscale-engine-sql.atscale.svc.cluster.local",
}


@dag(schedule="0 2 * * *", start_date=datetime(2026, 8, 1), catchup=False,
     tags=["sml-cicd-demo"], doc_md=__doc__)
def sml_regression_nightly():

    @task
    def run_parity():
        from airflow.exceptions import AirflowSkipException
        from airflow.models import Variable

        user = Variable.get("atscale_sql_user", default_var=None)
        password = Variable.get("atscale_sql_password", default_var=None)
        if not user or not password:
            raise AirflowSkipException(
                "atscale_sql_user/atscale_sql_password not set — skipping parity run.")

        import psycopg2
        results = {}
        for env, host in ENGINES.items():
            conn = psycopg2.connect(host=host, port=15432, dbname=CATALOG,
                                    user=user, password=password,
                                    connect_timeout=30)
            with conn, conn.cursor() as cur:
                results[env] = []
                for q in QUERIES:
                    cur.execute(q)
                    results[env].append(cur.fetchall())
            conn.close()

        mismatches = [i for i, (a, b) in
                      enumerate(zip(results["qa"], results["live"]))
                      if a != b]
        for i, q in enumerate(QUERIES):
            flag = "MISMATCH" if i in mismatches else "ok"
            print(f"[{flag}] {q}\n  qa={results['qa'][i]}  live={results['live'][i]}")
        assert not mismatches, f"{len(mismatches)} parity mismatch(es) QA vs Live"

    run_parity()


sml_regression_nightly()
