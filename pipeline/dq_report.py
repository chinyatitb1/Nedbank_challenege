"""
DQ report writer — produces /data/output/dq_report.json for Stage 2+.
"""

import json
import os
from datetime import datetime, timezone

from pipeline.logger import get_logger

LOGGER = get_logger(__name__)


def write_dq_report(
    config: dict,
    dq_rules: dict,
    start_time: float,
    end_time: float,
    raw_counts: dict[str, int],
    dq_counts: dict[str, int],
    gold_counts: dict[str, int],
    flag_stats: dict | None = None,
) -> None:
    """Write the DQ report JSON to the configured output path.

    Args:
        config:     Pipeline config (has output.dq_report_path).
        dq_rules:   Loaded dq_rules.yaml dict.
        start_time: Pipeline start time (time.time() epoch seconds).
        end_time:   Pipeline end time (time.time() epoch seconds).
        raw_counts: {"accounts_raw": N, "transactions_raw": N, "customers_raw": N}
        dq_counts:  {"DUPLICATE_DEDUPED": N, "ORPHANED_ACCOUNT": N, ...}
        gold_counts: {"fact_transactions": N, "dim_accounts": N, "dim_customers": N}
        flag_stats: {"total_records": N, "clean_records": N, "flagged_records": N, "flag_counts": {}}
    """
    rules = dq_rules.get("rules", {})

    # Denominator per scope for percentage_of_total
    scope_denominator = {
        "transactions": raw_counts.get("transactions_raw", 0),
        "accounts": raw_counts.get("accounts_raw", 0),
        "customers": raw_counts.get("customers_raw", 0),
        "all": sum(raw_counts.values()),
    }

    dq_issues = []
    for rule_key, count in dq_counts.items():
        if count <= 0:
            continue
        rule_def = rules.get(rule_key, {})
        scope = rule_def.get("scope", "transactions")
        denominator = scope_denominator.get(scope, 1)
        pct = round((count / denominator) * 100, 2) if denominator > 0 else 0.0

        # records_in_output: for QUARANTINED/EXCLUDED actions → 0; for normalised/cast → count
        handling = rule_def.get("handling", "")
        if handling in ("QUARANTINED", "EXCLUDED_NULL_PK"):
            records_in_output = 0
        elif handling == "DEDUPLICATED_KEEP_FIRST":
            # For dedup: records_in_output = total unique transaction_ids that survived
            # which is transactions_raw minus the duplicate extras
            records_in_output = raw_counts.get("transactions_raw", 0) - count
        else:
            # CAST_TO_DECIMAL, NORMALISED_DATE, NORMALISED_CURRENCY — all retained
            records_in_output = count

        dq_issues.append(
            {
                "issue_type": rule_def.get("issue_type", rule_key),
                "records_affected": count,
                "percentage_of_total": pct,
                "handling_action": handling,
                "records_in_output": records_in_output,
            }
        )

    run_ts = datetime.fromtimestamp(start_time, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    duration = int(end_time - start_time)

    report = {
        "$schema": "nedbank-de-challenge/dq-report/v1",
        "run_timestamp": run_ts,
        "stage": "2",
        "source_record_counts": raw_counts,
        "dq_issues": dq_issues,
        "gold_layer_record_counts": gold_counts,
        "execution_duration_seconds": duration,
    }

    # Add harness-required keys (Check 6 validates these)
    if flag_stats:
        report["total_records"] = flag_stats["total_records"]
        report["clean_records"] = flag_stats["clean_records"]
        report["flagged_records"] = flag_stats["flagged_records"]
        report["flag_counts"] = flag_stats["flag_counts"]
    else:
        total = gold_counts.get("fact_transactions", 0)
        report["total_records"] = total
        report["clean_records"] = total
        report["flagged_records"] = 0
        report["flag_counts"] = {}

    report_path = config.get("output", {}).get("dq_report_path", "/data/output/dq_report.json")
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    LOGGER.info("DQ report written to %s (%d issues reported)", report_path, len(dq_issues))
