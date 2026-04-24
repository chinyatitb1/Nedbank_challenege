"""
Pipeline entry point.
"""

import os
import sys
import time

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.config import load_config, load_dq_rules
from pipeline.dq_report import write_dq_report
from pipeline.ingest import run_ingestion
from pipeline.provision import run_provisioning
from pipeline.spark_utils import stop_spark_session
from pipeline.transform import run_transformation


if __name__ == "__main__":
    start_time = time.time()
    try:
        raw_counts = run_ingestion()
        dq_counts_transform = run_transformation()
        gold_counts, dq_counts_provision, flag_stats = run_provisioning()

        # Merge DQ counts from transform and provision stages
        dq_counts = {**dq_counts_transform, **dq_counts_provision}

        end_time = time.time()
        config = load_config()
        dq_rules = load_dq_rules()
        write_dq_report(config, dq_rules, start_time, end_time, raw_counts, dq_counts, gold_counts, flag_stats)
    finally:
        stop_spark_session()
