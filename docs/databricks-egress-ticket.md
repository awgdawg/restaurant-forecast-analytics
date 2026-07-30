# Support ticket draft: serverless egress blocked despite "allow all" network policy

Draft for a Databricks support case (file via the in-workspace Help menu →
Support, or https://help.databricks.com → Submit Case). Everything below the
line is the proposed ticket body; fill in the bracketed fields before sending.

---

**Subject:** Serverless egress DNS-blocked despite account network policy
"Allow access to all destinations" (upgraded trial workspace)

**Workspace:** `dbc-<workspace>.cloud.databricks.com` (workspace ID
`<workspace-id>`)

**Account email:** [account owner email]

**Plan/billing:** Started as a 14-day trial (express setup); a payment method
has been added and the account upgraded. The account console network policy
for this workspace shows **"Allow access to all destinations."**

## What we are trying to do

A scheduled Databricks Job (`restaurant-forecast-nightly`, job ID
`<job-id>`) runs Python wheel tasks on serverless compute. The first
task must call a third-party REST API — Toast POS at
`ws-api.toasttab.com` — to ingest point-of-sale data.

## Problem

Inside serverless job compute, DNS resolution fails for any host that is not
on what appears to be an internal allowlist. This persists after adding a
payment method, and it contradicts the account-level network policy, which is
set to allow all destinations.

## Evidence

Reproducible probe (one-time serverless run, `spark_python_task`, environment
client `"2"`, no credentials involved): run ID `<run-id>` on
2026-07-07, run page:
`https://dbc-<workspace>.cloud.databricks.com/?o=<workspace-id>#job/<probe-job-id>/run/<run-id>`

Output, verbatim:

```
=== DNS resolution ===
DNS example.com: FAIL gaierror: [Errno -3] Temporary failure in name resolution
DNS pypi.org: 151.101.64.223
DNS ws-api.toasttab.com: FAIL gaierror: [Errno -3] Temporary failure in name resolution
DNS sheets.googleapis.com: 192.168.200.20
DNS oauth2.googleapis.com: 192.168.200.20
DNS www.googleapis.com: 192.168.200.20

=== HTTPS reachability (10s timeout) ===
HTTPS https://ws-api.toasttab.com/authentication/v1/authentication/login: FAIL URLError: <urlopen error [Errno -3] Temporary failure in name resolution>
HTTPS https://example.com/: FAIL URLError: <urlopen error [Errno -3] Temporary failure in name resolution>
HTTPS https://pypi.org/simple/: HTTP 200
```

Interpretation:

- Allowlisted infrastructure hosts work: `pypi.org` resolves publicly and
  serves HTTP 200; `*.googleapis.com` resolves to an internal proxy
  (`192.168.200.20`).
- General internet does not: both `ws-api.toasttab.com` and the neutral
  control `example.com` fail DNS resolution entirely, so this is not
  destination-specific filtering of Toast.
- An identical probe on 2026-07-06 (before and after adding the payment
  method) produced the same fingerprint — adding billing did not change
  egress behavior.
- Additionally, the workspace's **own public SQL warehouse hostname** is
  unreachable from serverless jobs (`databricks-sql-connector` thrift
  `OpenSession` exhausts network retries), observed 2026-07-06. We have since
  moved all in-job I/O to Spark/Unity Catalog, so this part is informational.

## Request

1. Enable full serverless egress for this workspace so it honors the
   account's "Allow access to all destinations" network policy; **or**
2. Add `ws-api.toasttab.com` (Toast POS API) to this workspace's serverless
   egress allowlist; **or**
3. If neither is possible on this account tier, please state which plan or
   configuration (e.g., customer-managed VPC / classic compute) is required
   for serverless outbound internet access, so we can plan accordingly.

## Impact

The ingestion task cannot run in the cloud and currently runs on an on-prem
machine as a workaround. All other job tasks (Spark load, dbt, forecasting)
run green on serverless. Restoring in-cloud ingestion on our side is a
one-line config change once egress is available.
