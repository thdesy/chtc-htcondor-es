#!/usr/bin/env python
"""
Script for processing the contents of the CMS pool.
"""

import os
import sys
import time
import signal
import logging
import argparse
import multiprocessing

try:
    import htcondor_es
except ImportError:
    if os.path.exists("src/htcondor_es/__init__.py") and "src" not in sys.path:
        sys.path.append("src")

import htcondor_es.history
import htcondor_es.queues
from htcondor_es.utils import default_config, load_config
from htcondor_es.utils import get_schedds, set_up_logging, send_email_alert
from htcondor_es.utils import collect_metadata, TIMEOUT_MINS


def main_driver(args):
    """
    Driver method for the spider script.
    """
    starttime = time.time()

    signal.alarm(TIMEOUT_MINS * 60 + 60)

    # Get all the schedd ads
    schedd_ads = []
    schedd_ads = get_schedds(args)
    logging.warning("&&& There are %d schedds to query.", len(schedd_ads))

    pool = multiprocessing.Pool(processes=args.process_parallel_queries)

    metadata = collect_metadata()

    if args.process_schedd_history:
        htcondor_es.history.process_histories(
            schedd_ads=schedd_ads,
            starttime=starttime,
            pool=pool,
            args=args,
            metadata=metadata,
        )

    # Now that we have the fresh history, process the queues themselves.
    if args.process_schedd_queue:
        htcondor_es.queues.process_queues(
            schedd_ads=schedd_ads,
            starttime=starttime,
            pool=pool,
            args=args,
            metadata=metadata,
        )

    pool.close()
    pool.join()

    logging.warning(
        "@@@ Total processing time: %.2f mins", ((time.time() - starttime) / 60.0)
    )

    return 0


def main():
    """
    Main method for the spider_cms script.

    Parses arguments and invokes main_driver
    """
    defaults = default_config()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config_file",
        dest="config_file",
        help=(
            "File containing configuration for the spider script. "
            "Config file values will be overridden by commandline arguments."
        ),
    )
    parser.add_argument(
        "--collectors",
        dest="collectors",
        help="Comma-separated list of Collector addresses used to locate Schedds",
    )
    parser.add_argument(
        "--schedds",
        dest="schedds",
        help=(
            "Comma-separated list of Schedd names to process "
            "[default is to process all Schedds located by Collectors]"
        ),
    )
    parser.add_argument(
        "--skip_process_schedd_history",
        action="store_const",
        const=False,
        dest="process_schedd_history",
        help="Do not process Schedd history"
    )
    parser.add_argument(
        "--process_schedd_queue",
        action="store_const",
        const=True,
        dest="process_schedd_queue",
        help="Process Schedd queue (Running/Idle/Pending jobs)",
    )
    parser.add_argument(
        "--process_max_documents",
        type=int,
        dest="process_max_documents",
        help=(
            "Abort after this many documents (per Schedd). "
            "[default: {} (process all)]".format(defaults['process_max_documents'])
        ),
    )
    parser.add_argument(
        "--process_parallel_queries",
        type=int,
        dest="process_parallel_queries",
        help=(
            "Number of parallel processes for querying "
            "[default: {}]".format(defaults['process_parallel_queries'])
        ),
    )
    parser.add_argument(
        "--es_host",
        dest="es_host",
        help=(
            "Host of the Elasticsearch instance to be used "
            "[default: {}]".format(defaults['es_host'])
        ),
    )
    parser.add_argument(
        "--es_port",
        type=int,
        dest="es_port",
        help=(
            "Port of the Elasticsearch instance to be used "
            "[default: {}]".format(defaults['es_port'])
        )
    )
    parser.add_argument(
        "--es_bunch_size",
        type=int,
        dest="es_bunch_size",
        help=(
            "Send docs to ES in bunches of this number "
            "[default: {}]".format(defaults['es_bunch_size'])
        )
    )
    parser.add_argument(
        "--es_feed_schedd_history",
        action="store_const",
        const=True,
        dest="es_feed_schedd_history",
        help="Feed Schedd history to Elasticsearch"
    )
    parser.add_argument(
        "--es_feed_schedd_queue",
        action="store_const",
        const=True,
        dest="es_feed_schedd_queue",
        help="Feed Schedd queue to Elasticsearch",
    )
    parser.add_argument(
        "--es_index_name",
        dest="es_index_name",
        help=(
            "Trunk of Elasticsearch index name. "
            "[default: {}]".format(defaults['es_index_name'])
        ),
    )
    parser.add_argument(
        "--es_index_date_attr",
        dest="es_index_date_attr",
        help=(
            "Job attribute to use as date for Elasticsearch index name "
            "[default: {}]".format(defaults['es_index_date_attr'])
        ),
    )
    parser.add_argument(
        "--query_queue_batch_size",
        default=50,
        type=int,
        dest="query_queue_batch_size",
        help=(
            "Send docs to listener in batches of this number "
            "[default: %(default)d]"
        ),
    )
    parser.add_argument(
        "--keep_full_queue_data",
        action="store_true",
        dest="keep_full_queue_data",
        help="Drop all but some fields for running jobs.",
    )
    parser.add_argument(
        "--upload_pool_size",
        default=8,
        type=int,
        dest="upload_pool_size",
        help=(
            "Number of parallel processes for uploading "
            "[default: %(default)d]"
        ),
    )
    parser.add_argument(
        "--log_dir",
        default="log/",
        type=str,
        dest="log_dir",
        help=(
            "Directory for logging information "
            "[default: %(default)s]"
        ),
    )
    parser.add_argument(
        "--log_level",
        default="WARNING",
        type=str,
        dest="log_level",
        help=(
            "Log level (CRITICAL/ERROR/WARNING/INFO/DEBUG) "
            "[default: %(default)s]"
        ),
    )
    parser.add_argument(
        "--email_alerts",
        default=[],
        action="append",
        dest="email_alerts",
        help=(
            "Email addresses for alerts "
            "[default: none]"
        ),
    )
    parser.add_argument(
        "--read_only",
        action="store_true",
        dest="read_only",
        help="Only read the info, don't submit it.",
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        dest="dry_run",
        help=(
            "Don't even read info, just pretend to. (Still "
            "query the collector for the Schedds though.)"
        ),
    )
    args = parser.parse_args()
    args = load_config(args)
    set_up_logging(args)

    # --dry_run implies read_only
    args.read_only = args.read_only or args.dry_run
    main_driver(args)


if __name__ == "__main__":
    main()
