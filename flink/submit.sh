#!/usr/bin/env bash
# Submits each job in /opt/flink/sql/jobs/ unless a job with that name is
# already on the cluster. Runs as a one-shot Compose service on every `up`, so
# it must be safe to run twice: without the check, each `up` would add another
# copy of every job, and two passthroughs double-write every row.
#
# A job's name is its file name; each job file sets pipeline.name to match.
set -euo pipefail
cd /opt/flink

# Fails the script, and so the service, if the JobManager is unreachable.
listed=$(bin/flink list -r)

for job in sql/jobs/*.sql; do
  name=$(basename "$job" .sql)
  if grep -qF ": ${name} (" <<<"$listed"; then
    echo "skip   ${name}: already on the cluster"
    continue
  fi
  echo "submit ${name}"
  bin/sql-client.sh -i sql/init.sql -f "$job"
done
