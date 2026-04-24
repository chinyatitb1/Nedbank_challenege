"""
Pipeline entry point.
"""

import os
import sys

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.ingest import run_ingestion
from pipeline.provision import run_provisioning
from pipeline.spark_utils import stop_spark_session
from pipeline.transform import run_transformation


if __name__ == "__main__":
    try:
        run_ingestion()
        run_transformation()
        run_provisioning()
    finally:
        stop_spark_session()
