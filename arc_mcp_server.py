#!/usr/bin/env python3
"""
Oracle Account Reconciliation (ARC) MCP Server - POC

Covers two ARC modules:
  - Transaction Matching  (/arm/rest/v1/...)
  - Reconciliation Compliance  (/armARCS/rest/v1/...)

Auth: HTTP Basic via env vars ARC_BASE_URL, ARC_USERNAME, ARC_PASSWORD.

Usage:
    export ARC_BASE_URL=https://your-tenant.epm.us2.oraclecloud.com
    export ARC_USERNAME=your.user@company.com
    export ARC_PASSWORD=your_password
    python arc_mcp_server.py
"""

import asyncio
import base64
import json
import os
import time
from typing import Any, Dict, List, Optional

import httpx
from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, ConfigDict, Field, field_validator

# ---------------------------------------------------------------------------
# Server init
# ---------------------------------------------------------------------------

mcp = FastMCP("arc_mcp")

# ---------------------------------------------------------------------------
# Constants / config
# ---------------------------------------------------------------------------

ARC_BASE_URL   = os.getenv("ARC_BASE_URL", "").rstrip("/")
ARC_USERNAME   = os.getenv("ARC_USERNAME", "")
ARC_PASSWORD   = os.getenv("ARC_PASSWORD", "")

# Module-specific base paths
TM_BASE   = f"{ARC_BASE_URL}/arm/rest/v1"          # Transaction Matching
RC_BASE   = f"{ARC_BASE_URL}/armARCS/rest/v1"       # Reconciliation Compliance
FILE_BASE = f"{ARC_BASE_URL}/interop/rest/v2"       # File upload/download

POLL_INTERVAL = 5   # seconds between job status polls
POLL_TIMEOUT  = 300 # max seconds to wait for a job

# ---------------------------------------------------------------------------
# Shared HTTP helpers
# ---------------------------------------------------------------------------

def _auth() -> tuple[str, str]:
    return (ARC_USERNAME, ARC_PASSWORD)


def _headers(content_type: str = "application/json") -> Dict[str, str]:
    return {
        "Content-Type": content_type,
        "Accept": "application/json",
    }


async def _post_job(base: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """POST a job to TM or RC and return the parsed response body."""
    async with httpx.AsyncClient(auth=_auth(), timeout=60.0) as client:
        r = await client.post(
            f"{base}/jobs",
            json=payload,
            headers=_headers(),
        )
        r.raise_for_status()
        # Some endpoints return empty body (204 / fire-and-forget)
        if not r.content or r.status_code == 204:
            return {"status": "SUCCESS", "statusMessage": "Accepted (no body)"}
        return r.json()


async def _get_job(base: str, job_id: str) -> Dict[str, Any]:
    """Poll a single job status."""
    async with httpx.AsyncClient(auth=_auth(), timeout=30.0) as client:
        r = await client.get(
            f"{base}/jobs/{job_id}",
            headers=_headers(),
        )
        r.raise_for_status()
        if not r.content:
            return {"status": "UNKNOWN"}
        return r.json()


async def _poll_job(base: str, job_id: str) -> Dict[str, Any]:
    """Poll until job reaches a terminal state or POLL_TIMEOUT is hit."""
    terminal = {"SUCCESS", "ERROR", "FAILED", "COMPLETE", "COMPLETED"}
    deadline = time.time() + POLL_TIMEOUT
    while time.time() < deadline:
        data = await _get_job(base, job_id)
        status = (data.get("status") or data.get("jobStatus") or "").upper()
        if status in terminal:
            return data
        await asyncio.sleep(POLL_INTERVAL)
    return {"status": "TIMEOUT", "statusMessage": f"Job {job_id} did not complete within {POLL_TIMEOUT}s"}


def _fmt_job(data: Dict[str, Any], label: str = "Job") -> str:
    """Pretty-print a job result dict."""
    return json.dumps({label: data}, indent=2)


def _handle_error(e: Exception) -> str:
    if isinstance(e, httpx.HTTPStatusError):
        body = ""
        try:
            body = e.response.text
        except Exception:
            pass
        return (
            f"Error {e.response.status_code}: {e.response.reason_phrase}\n"
            f"URL: {e.request.url}\nBody: {body}"
        )
    if isinstance(e, httpx.TimeoutException):
        return "Error: Request timed out. Check network / VPN."
    if isinstance(e, httpx.ConnectError):
        return "Error: Cannot connect. Check ARC_BASE_URL and network."
    return f"Error ({type(e).__name__}): {e}"


# ---------------------------------------------------------------------------
# Pydantic input models
# ---------------------------------------------------------------------------

class _Base(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, validate_assignment=True)


# ----- File upload -----------------------------------------------------------

class UploadFileInput(_Base):
    local_path: Optional[str] = Field(
        default=None,
        description="Absolute local path to the file on the MCP server machine (e.g. /tmp/data.csv or C:\\data.csv).",
    )
    file_content_b64: Optional[str] = Field(
        default=None,
        description="Base64-encoded file content. Use when the file is generated in-memory rather than saved to disk.",
    )
    filename: str = Field(
        ...,
        description="Target filename in the ARC inbox (e.g. 'profiles.csv'). Must include extension.",
    )

    @field_validator("filename")
    @classmethod
    def no_slashes(cls, v: str) -> str:
        if "/" in v or "\\" in v:
            raise ValueError("filename must be a bare filename, not a path")
        return v


# ----- Transaction Matching --------------------------------------------------

class ImportMatchTypeInput(_Base):
    filename: str = Field(..., description="Name of the zip already in the ARC inbox.")
    import_mode: str = Field(default="CREATE", description="'CREATE' or 'REPLACE'.")


class ImportTransactionsInput(_Base):
    match_type_id: str = Field(..., description="Match type TEXT_ID, e.g. 'DPR_PAY_GL_BANK'.")
    data_source: str = Field(..., description="Data source TEXT_ID, e.g. 'GL' or 'Bank'.")
    filename: str = Field(..., description="CSV filename already in the ARC inbox.")
    date_format: str = Field(default="dd-MMM-yy", description="Date format used in the CSV.")


class ImportBalancesInput(_Base):
    match_type_id: str = Field(..., description="Match type TEXT_ID.")
    data_source: str = Field(..., description="Data source TEXT_ID.")
    filename: str = Field(..., description="CSV filename in the ARC inbox.")
    date_format: str = Field(default="dd-MMM-yy", description="Date format.")


class RunAutoMatchInput(_Base):
    match_type_id: str = Field(..., description="Match type TEXT_ID.")


class UnmatchInput(_Base):
    match_type_id: str = Field(..., description="Match type TEXT_ID.")
    match_ids: List[int] = Field(..., description="List of match IDs to unmatch (up to 10,000).")
    force_reopen: bool = Field(default=False, description="Reopen reconciliations if accounting date < closed-through date.")


class PurgeInput(_Base):
    match_type_id: str = Field(..., description="Match type TEXT_ID.")
    age_days: int = Field(..., description="Purge transactions matched >= this many days ago.", ge=1)
    filter_operator: Optional[str] = Field(default=None, description="EQUALS, NOT_EQUALS, STARTS_WITH, etc.")
    filter_values: Optional[List[str]] = Field(default=None, description="Account filter values.")
    log_filename: Optional[str] = Field(default=None, description="Optional log file saved to outbox.")


class AutoAlertInput(_Base):
    match_type_id: str = Field(..., description="Match type TEXT_ID.")


class GetJobInput(_Base):
    job_id: str = Field(..., description="TM job ID returned by a previous operation.")


class UpdateTransactionInput(_Base):
    match_type_id: str = Field(..., description="Match type TEXT_ID.")
    recon_id: str = Field(..., description="Reconciliation TEXT_ID.")
    data_source: str = Field(..., description="Data source TEXT_ID.")
    transaction_id: str = Field(..., description="Transaction ID of the unmatched transaction.")
    attribute_id: str = Field(..., description="Attribute TEXT_ID to update, e.g. 'GL_AMOUNT'.")
    value: str = Field(..., description="New value for the attribute.")
    calculate: bool = Field(default=False, description="Recalculate calculated attributes after update.")
    force_reopen: bool = Field(default=False, description="Reopen reconciliation if needed.")


# ----- Reconciliation Compliance ---------------------------------------------

class ImportProfilesInput(_Base):
    filename: str = Field(
        ...,
        description=(
            "CSV filename already in the ARC inbox. "
            "Columns: Account ID, Name, Format, Preparer, Reviewer 1, Due Date, Frequency, Currency, ..."
        ),
    )
    import_type: str = Field(
        default="Replace",
        description="'Replace' (upsert by Account ID) or 'ReplaceAll' (delete then reload).",
    )
    date_format: str = Field(default="MM/DD/YYYY", description="Date format in the CSV.")


class CreateReconciliationsInput(_Base):
    period_name: str = Field(..., description="Period to instantiate profiles into, e.g. 'Jan-2026'.")
    filter: Optional[str] = Field(default=None, description="Optional filter name; omit to create for all profiles.")


class ImportRCBalancesInput(_Base):
    period_name: str = Field(..., description="Period, e.g. 'Jan-2026'.")
    filename: str = Field(..., description="CSV filename in inbox. Columns: Account ID, Amount, Currency.")
    balance_type: str = Field(
        default="SRC",
        description="'SRC' = source system balance (GL), 'SUB' = sub-system / supporting balance.",
    )
    currency_bucket: str = Field(default="Functional", description="Currency bucket, e.g. 'Functional' or 'Entered'.")
    date_format: str = Field(default="MM/DD/YYYY", description="Date format in the CSV.")


class ChangePeriodStatusInput(_Base):
    period_name: str = Field(..., description="Period name, e.g. 'Jan-2026'.")
    status: str = Field(..., description="'OPEN' or 'CLOSED'.")

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        v = v.upper()
        if v not in ("OPEN", "CLOSED"):
            raise ValueError("status must be OPEN or CLOSED")
        return v


class GetRCJobInput(_Base):
    job_id: str = Field(..., description="RC job ID returned by a previous RC operation.")


# ---------------------------------------------------------------------------
# ── FILE UPLOAD ──────────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

@mcp.tool(
    name="arc_upload_file",
    annotations={
        "title": "Upload File to ARC Inbox",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True,
    },
)
async def arc_upload_file(params: UploadFileInput) -> str:
    """Upload a file to the Oracle ARC inbox so it can be consumed by import jobs."""
    if not params.local_path and not params.file_content_b64:
        return "Error: Provide either local_path or file_content_b64."
    if params.local_path and params.file_content_b64:
        return "Error: Provide only one of local_path or file_content_b64, not both."

    try:
        if params.local_path:
            with open(params.local_path, "rb") as fh:
                content = fh.read()
        else:
            content = base64.b64decode(params.file_content_b64)

        upload_url = f"{ARC_BASE_URL}/interop/rest/11.1.2.3.600/applicationsnapshots/{params.filename}/contents"
        async with httpx.AsyncClient(auth=_auth(), timeout=120.0, follow_redirects=True) as client:
            r = await client.post(
                upload_url,
                content=content,
                headers={"Content-Type": "application/octet-stream", "Accept": "application/json"},
            )
            r.raise_for_status()

        return json.dumps({"status": "SUCCESS", "filename": params.filename}, indent=2)
    except FileNotFoundError:
        return f"Error: File not found: {params.local_path}"
    except Exception as e:
        return _handle_error(e)


# ---------------------------------------------------------------------------
# ── TRANSACTION MATCHING tools ───────────────────────────────────────────────
# ---------------------------------------------------------------------------

@mcp.tool(
    name="arc_connection_test",
    annotations={"title": "Test ARC Connectivity", "readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def arc_connection_test() -> str:
    """Test connectivity to Oracle ARC by listing inbox files."""
    try:
        async with httpx.AsyncClient(auth=_auth(), timeout=30.0, follow_redirects=True) as client:
            r = await client.get(
                f"{ARC_BASE_URL}/armARCS/rest",
                headers=_headers(),
            )
            r.raise_for_status()
            try:
                data = r.json() if r.content else {}
            except Exception:
                data = {}
        files = data.get("files", [])
        return json.dumps({"status": "CONNECTED", "inbox_file_count": len(files), "files": files}, indent=2)
    except Exception as e:
        return _handle_error(e)


@mcp.tool(
    name="arc_list_files",
    annotations={"title": "List ARC Inbox Files", "readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def arc_list_files() -> str:
    """List all files currently in the ARC inbox/outbox."""
    try:
        async with httpx.AsyncClient(auth=_auth(), timeout=30.0) as client:
            r = await client.get(
                f"{FILE_BASE}/files",
                params={"extDirPath": "inbox"},
                headers=_headers(),
            )
            r.raise_for_status()
            data = r.json() if r.content else {}
        return json.dumps(data, indent=2)
    except Exception as e:
        return _handle_error(e)


@mcp.tool(
    name="arc_delete_file",
    annotations={"title": "Delete File from ARC Inbox", "readOnlyHint": False, "destructiveHint": True, "idempotentHint": False, "openWorldHint": True},
)
async def arc_delete_file(filename: str) -> str:
    """Delete a named file from the ARC inbox."""
    try:
        async with httpx.AsyncClient(auth=_auth(), timeout=30.0) as client:
            r = await client.delete(
                f"{FILE_BASE}/files/{filename}",
                params={"extDirPath": "inbox"},
                headers=_headers(),
            )
            r.raise_for_status()
        return json.dumps({"status": "DELETED", "filename": filename}, indent=2)
    except Exception as e:
        return _handle_error(e)


@mcp.tool(
    name="arc_import_match_type",
    annotations={"title": "Import Match Type (TM)", "readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
)
async def arc_import_match_type(params: ImportMatchTypeInput) -> str:
    """Import a Transaction Matching match type from a zip file in the ARC inbox."""
    try:
        payload = {
            "jobName": "IMPORT_MATCH_TYPE",
            "parameters": {
                "fileName": params.filename,
                "importMode": params.import_mode,
            },
        }
        result = await _post_job(TM_BASE, payload)
        job_id = result.get("jobId") or result.get("id")
        if job_id:
            result = await _poll_job(TM_BASE, str(job_id))
        return _fmt_job(result, "ImportMatchType")
    except Exception as e:
        return _handle_error(e)


@mcp.tool(
    name="arc_import_transactions",
    annotations={"title": "Import Pre-Mapped Transactions (TM)", "readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
)
async def arc_import_transactions(params: ImportTransactionsInput) -> str:
    """Import pre-mapped transactions CSV into a Transaction Matching data source."""
    try:
        payload = {
            "jobName": "importtmpremappedtransactions",
            "parameters": {
                "reconciliationType": params.match_type_id,
                "dataSource": params.data_source,
                "file": params.filename,
                "dateFormat": params.date_format,
            },
        }
        result = await _post_job(TM_BASE, payload)
        job_id = result.get("jobId") or result.get("id")
        if job_id:
            result = await _poll_job(TM_BASE, str(job_id))
        return _fmt_job(result, "ImportTransactions")
    except Exception as e:
        return _handle_error(e)


@mcp.tool(
    name="arc_import_balances",
    annotations={"title": "Import Pre-Mapped Balances (TM)", "readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
)
async def arc_import_balances(params: ImportBalancesInput) -> str:
    """Import pre-mapped balances CSV into a Transaction Matching data source."""
    try:
        payload = {
            "jobName": "IMPORT_PRE_MAPPED_BALANCES",
            "parameters": {
                "reconciliationType": params.match_type_id,
                "dataSource": params.data_source,
                "file": params.filename,
                "dateFormat": params.date_format,
            },
        }
        result = await _post_job(TM_BASE, payload)
        job_id = result.get("jobId") or result.get("id")
        if job_id:
            result = await _poll_job(TM_BASE, str(job_id))
        return _fmt_job(result, "ImportBalances")
    except Exception as e:
        return _handle_error(e)


@mcp.tool(
    name="arc_run_automatch",
    annotations={"title": "Run Auto-Match (TM)", "readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
)
async def arc_run_automatch(params: RunAutoMatchInput) -> str:
    """Run the auto-match process for a Transaction Matching match type."""
    try:
        payload = {
            "jobName": "runautomatch",
            "parameters": {"reconTypeId": params.match_type_id},
        }
        result = await _post_job(TM_BASE, payload)
        job_id = result.get("jobId") or result.get("id")
        if job_id:
            result = await _poll_job(TM_BASE, str(job_id))
        return _fmt_job(result, "AutoMatch")
    except Exception as e:
        return _handle_error(e)


# ---------------------------------------------------------------------------
# FIXED: Unmatch now uses /arm/tmapi/ endpoints (captured from browser)
# Old code posted to /arm/rest/v1/jobs with jobName "UNMATCH" which is invalid.
# Correct endpoints:
#   POST /arm/tmapi/unmatched/match/{matchId}
#   POST /arm/tmapi/bulkTransaction/reverseStatus/{matchId}
# ---------------------------------------------------------------------------

@mcp.tool(
    name="arc_unmatch_transactions",
    annotations={
        "title": "Unmatch Transactions (TM)",
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": True,
    },
)
async def arc_unmatch_transactions(params: UnmatchInput) -> str:
    """Unmatch previously matched transactions by their match IDs."""
    try:
        results = []
        async with httpx.AsyncClient(auth=_auth(), timeout=60.0, follow_redirects=True) as client:
            for match_id in params.match_ids:
                try:
                    # Step 1: Reverse status
                    reverse_url = f"{ARC_BASE_URL}/arm/tmapi/bulkTransaction/reverseStatus/{match_id}"
                    r1 = await client.post(
                        reverse_url,
                        headers=_headers(),
                    )
                    r1.raise_for_status()

                    # Step 2: Unmatch
                    unmatch_url = f"{ARC_BASE_URL}/arm/tmapi/unmatched/match/{match_id}"
                    r2 = await client.post(
                        unmatch_url,
                        headers=_headers(),
                    )
                    r2.raise_for_status()

                    results.append({
                        "matchId": match_id,
                        "status": "UNMATCHED",
                        "reverseStatus": r1.status_code,
                        "unmatchStatus": r2.status_code,
                    })
                except httpx.HTTPStatusError as he:
                    results.append({
                        "matchId": match_id,
                        "status": "ERROR",
                        "error": f"{he.response.status_code}: {he.response.text[:200]}",
                    })

        return json.dumps({"Unmatch": {"results": results, "total": len(results)}}, indent=2)
    except Exception as e:
        return _handle_error(e)


@mcp.tool(
    name="arc_update_transaction",
    annotations={"title": "Update Transaction Attribute (TM)", "readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def arc_update_transaction(params: UpdateTransactionInput) -> str:
    """Update an editable attribute on an unmatched transaction before re-running automatch."""
    try:
        payload = {
            "jobName": "UPDATE_TRANSACTION",
            "parameters": {
                "matchTypeId": params.match_type_id,
                "reconId": params.recon_id,
                "dataSourceId": params.data_source,
                "transactionId": params.transaction_id,
                "attributeId": params.attribute_id,
                "value": params.value,
                "calculate": params.calculate,
                "forceReopen": params.force_reopen,
            },
        }
        result = await _post_job(TM_BASE, payload)
        job_id = result.get("jobId") or result.get("id")
        if job_id:
            result = await _poll_job(TM_BASE, str(job_id))
        return _fmt_job(result, "UpdateTransaction")
    except Exception as e:
        return _handle_error(e)


@mcp.tool(
    name="arc_purge_transactions",
    annotations={"title": "Purge Old Matched Transactions (TM)", "readOnlyHint": False, "destructiveHint": True, "idempotentHint": False, "openWorldHint": True},
)
async def arc_purge_transactions(params: PurgeInput) -> str:
    """Purge matched transactions older than the specified number of days from a match type."""
    try:
        params_dict: Dict[str, Any] = {
            "matchTypeId": params.match_type_id,
            "ageDays": params.age_days,
        }
        if params.filter_operator:
            params_dict["filterOperator"] = params.filter_operator
        if params.filter_values:
            params_dict["filterValues"] = params.filter_values
        if params.log_filename:
            params_dict["logFileName"] = params.log_filename

        payload = {"jobName": "PURGE_TRANSACTIONS", "parameters": params_dict}
        result = await _post_job(TM_BASE, payload)
        job_id = result.get("jobId") or result.get("id")
        if job_id:
            result = await _poll_job(TM_BASE, str(job_id))
        return _fmt_job(result, "PurgeTransactions")
    except Exception as e:
        return _handle_error(e)


@mcp.tool(
    name="arc_run_auto_alert",
    annotations={"title": "Run Auto Alert (TM)", "readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
)
async def arc_run_auto_alert(params: AutoAlertInput) -> str:
    """Run the Auto Alert process to send email notifications about unmatched transactions."""
    try:
        payload = {
            "jobName": "AUTO_ALERT",
            "parameters": {"matchTypeId": params.match_type_id},
        }
        result = await _post_job(TM_BASE, payload)
        job_id = result.get("jobId") or result.get("id")
        if job_id:
            result = await _poll_job(TM_BASE, str(job_id))
        return _fmt_job(result, "AutoAlert")
    except Exception as e:
        return _handle_error(e)


@mcp.tool(
    name="arc_get_job_status",
    annotations={"title": "Get TM Job Status", "readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def arc_get_job_status(params: GetJobInput) -> str:
    """Check the current status of a Transaction Matching job."""
    try:
        result = await _get_job(TM_BASE, params.job_id)
        return _fmt_job(result, "JobStatus")
    except Exception as e:
        return _handle_error(e)


# ---------------------------------------------------------------------------
# ── RECONCILIATION COMPLIANCE tools ─────────────────────────────────────────
# ---------------------------------------------------------------------------

@mcp.tool(
    name="arc_import_profiles",
    annotations={
        "title": "Import RC Profiles (Reconciliation Compliance)",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": True,
    },
)
async def arc_import_profiles(params: ImportProfilesInput) -> str:
    """Import Account Reconciliation profiles from a CSV file."""
    try:
        payload = {
            "jobName": "IMPORT_PROFILES",
            "parameters": {
                "importType": params.import_type,
                "profileType": "Profiles",
                "fileLocation": params.filename,
                "dateFormat": params.date_format,
            },
        }
        result = await _post_job(RC_BASE, payload)
        job_id = result.get("jobId") or result.get("id")
        if job_id:
            result = await _poll_job(RC_BASE, str(job_id))
        return _fmt_job(result, "ImportProfiles")
    except Exception as e:
        return _handle_error(e)


@mcp.tool(
    name="arc_create_reconciliations",
    annotations={
        "title": "Create Reconciliations for Period (RC)",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": True,
    },
)
async def arc_create_reconciliations(params: CreateReconciliationsInput) -> str:
    """Instantiate profiles into actual reconciliation records for a specific period."""
    try:
        parameters: Dict[str, Any] = {"period": params.period_name}
        if params.filter:
            parameters["filter"] = params.filter
        payload = {"jobName": "CREATE_RECONCILIATIONS", "parameters": parameters}
        result = await _post_job(RC_BASE, payload)
        job_id = result.get("jobId") or result.get("id")
        if job_id:
            result = await _poll_job(RC_BASE, str(job_id))
        return _fmt_job(result, "CreateReconciliations")
    except Exception as e:
        return _handle_error(e)


@mcp.tool(
    name="arc_import_rc_balances",
    annotations={
        "title": "Import Balances into Reconciliation Compliance (RC)",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": True,
    },
)
async def arc_import_rc_balances(params: ImportRCBalancesInput) -> str:
    """Load GL or sub-system balances into Reconciliation Compliance for a period."""
    try:
        payload = {
            "jobName": "IMPORT_PREMAPPED_BALANCES",
            "parameters": {
                "period": params.period_name,
                "balanceType": params.balance_type,
                "currencyBucket": params.currency_bucket,
                "file": params.filename,
                "dateFormat": params.date_format,
            },
        }
        result = await _post_job(RC_BASE, payload)
        job_id = result.get("jobId") or result.get("id")
        if job_id:
            result = await _poll_job(RC_BASE, str(job_id))
        return _fmt_job(result, "ImportRCBalances")
    except Exception as e:
        return _handle_error(e)


@mcp.tool(
    name="arc_change_period_status",
    annotations={
        "title": "Open or Close a Reconciliation Period (RC)",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True,
    },
)
async def arc_change_period_status(params: ChangePeriodStatusInput) -> str:
    """Open or close a reconciliation period in Reconciliation Compliance."""
    try:
        payload = {
            "jobName": "CHANGE_PERIOD_STATUS",
            "parameters": {
                "period": params.period_name,
                "status": params.status,
            },
        }
        result = await _post_job(RC_BASE, payload)
        job_id = result.get("jobId") or result.get("id")
        if job_id:
            result = await _poll_job(RC_BASE, str(job_id))
        else:
            result["period"] = params.period_name
            result["newStatus"] = params.status
        return _fmt_job(result, "ChangePeriodStatus")
    except Exception as e:
        return _handle_error(e)


@mcp.tool(
    name="arc_get_rc_job_status",
    annotations={
        "title": "Get RC Job Status",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True,
    },
)
async def arc_get_rc_job_status(params: GetRCJobInput) -> str:
    """Check the current status of a Reconciliation Compliance job."""
    try:
        result = await _get_job(RC_BASE, params.job_id)
        return _fmt_job(result, "RCJobStatus")
    except Exception as e:
        return _handle_error(e)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

import contextlib
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

async def health(request):
    return JSONResponse(
        {
            "status": "ok",
            "service": "arc-mcp",
            "mcp_endpoint": "/mcp",
        }
    )

@contextlib.asynccontextmanager
async def lifespan(app: Starlette):
    async with mcp.session_manager.run():
        yield

app = Starlette(
    routes=[
        Route("/", endpoint=health),
        Mount("/mcp", app=mcp.streamable_http_app()),
    ],
    lifespan=lifespan,
)

if __name__ == "__main__":
    import uvicorn
    import os
    port = int(os.getenv("PORT", "10000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
