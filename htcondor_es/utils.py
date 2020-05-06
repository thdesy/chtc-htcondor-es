"""
Various helper utilities for the HTCondor-ES integration
"""

import os
import pwd
import sys
import time
import errno
import shlex
import socket
import random
import smtplib
import subprocess
import email.mime.text
import json
import logging
import logging.handlers
import configparser
from argparse import Namespace

import htcondor

TIMEOUT_MINS = 11


def default_config():
    defaults = {
        'process_schedd_history'   : False,
        'process_schedd_queue'     : False,
        'process_startd_history'   : False,
        'process_max_documents'    : 0,
        'process_parallel_queries' : 8,
        'es_host'                  : 'localhost',
        'es_port'                  : 9200,
        'es_bunch_size'            : 250,
        'es_feed_schedd_history'   : False,
        'es_feed_schedd_queue'     : False,
        'es_feed_startd_history'   : False,
        'es_index_name'            : 'htcondor_jobs',
        'es_index_date_attr'       : 'CompletionDate',
    }
    return defaults


def load_config(args):
    defaults = default_config()
    if (args is None) or (args.config_file is None):
        return args

    config = configparser.ConfigParser(
        allow_no_value=True,
        empty_lines_in_values=False)
    try:
        config_files = config.read(args.config_file)
        if len(config_files) == 0:
            # Something went wrong with reading the config file,
            # hopefully open() generates an informative exception.
            try:
                open(args.config_file, 'rb').close()
            except Exception:
                raise
            else:
                # open() didn't error, so something else happened *shrug*
                raise RuntimeError("Could not read config file. "
                    "Please check that it exists, is readable, and is free of "
                    "syntax errors.")
    except Exception:
        logging.exception("Fatal error while reading config file")
        sys.exit(1)

    # convert args from a namespace object to a dict
    args = vars(args)
    if (args.get('collectors') is None) and ('COLLECTORS' in config) and (len(list(config['COLLECTORS'])) > 0):
        args['collectors'] = ','.join(list(config['COLLECTORS']))
    if (args.get('schedds') is None)    and ('SCHEDDS' in config)    and (len(list(config['SCHEDDS'])) > 0):
        args['schedds']    = ','.join(list(config['SCHEDDS']))
    if (args.get('startds') is None)    and ('STARTDS' in config)    and (len(list(config['STARTDS'])) > 0):
        args['startds']    = ','.join(list(config['STARTDS']))
    if 'PROCESS' in config:
        process = config['PROCESS']
        if args.get('process_schedd_history') is None:
            args['process_schedd_history'] = process.getboolean(
                'schedd_history', fallback=defaults['process_schedd_history'])
        if args.get('process_schedd_queue') is None:
            args['process_schedd_queue'] = process.getboolean(
                'schedd_queue', fallback=defaults['process_schedd_queue'])
        if args.get('process_startd_history') is None:
            args['process_startd_history'] = process.getboolean(
                'startd_history', fallback=defaults['process_startd_history'])
        if args.get('process_max_documents') is None:
            args['process_max_documents'] = process.getint(
                'max_documents', fallback=defaults['process_max_documents'])
        if args.get('process_parallel_queries') is None:
            args['process_parallel_queries'] = process.getint(
                'parallel_queries', fallback=defaults['process_parallel_queries'])
    if 'ELASTICSEARCH' in config:
        es = config['ELASTICSEARCH']
        if args.get('es_host') is None:
            args['es_host'] = es.get('host', fallback=defaults['es_host'])
        if args.get('es_port') is None:
            args['es_port'] = es.get('port', fallback=defaults['es_port'])
        if args.get('es_username') is None:
            args['es_username'] = es.get('username', fallback=None)
        if args.get('es_password') is None:
            args['es_password'] = es.get('password', fallback=None)
        if args.get('es_use_https') is None:
            args['es_use_https'] = es.getboolean('use_https', fallback=False)
        if args.get('es_bunch_size') is None:
            args['es_bunch_size'] = es.getint(
                'bunch_size', fallback=defaults['es_bunch_size'])
        if args.get('es_feed_schedd_history') is None:
            args['es_feed_schedd_history'] = es.getboolean(
                'feed_schedd_history', fallback=defaults['es_feed_schedd_history'])
        if args.get('es_feed_schedd_queue') is None:
            args['es_feed_schedd_queue'] = es.getboolean(
                'feed_schedd_queue', fallback=defaults['es_feed_schedd_queue'])
        if args.get('es_feed_startd_history') is None:
            if args['es_feed_startd_history'] = es.getboolean(
                'feed_startd_history', fallback=defaults['es_feed_startd_history'])
        if args.get('es_index_name') is None:
            args['es_index_name'] = es.get(
                'index_name', fallback=defaults['es_index_name'])
        if args.get('es_index_date_attr') is None:
            args['es_index_date_attr'] = es.get(
                'index_date_attr', fallback=defaults['es_index_date_attr'])

    # convert args back to a namespace object
    args = Namespace(**args)
    return args


def get_schedds(args=None):
    """
    Return a list of schedd ads representing all the schedds in the pool.
    """
    collectors = args.collectors
    if collectors:
        collectors = collectors.split(',')
    else:
        collectors = []

    schedd_ads = {}
    for host in collectors:
        coll = htcondor.Collector(host)
        try:
            schedds = coll.locateAll(htcondor.DaemonTypes.Schedd)
        except IOError as e:
            logging.warning(str(e))
            continue

        for schedd in schedds:
            schedd["MyPool"] = host
            try:
                schedd_ads[schedd["Name"]] = schedd
            except KeyError:
                pass

    schedd_ads = list(schedd_ads.values())
    random.shuffle(schedd_ads)

    if args and args.schedds:
        return [s for s in schedd_ads if s["Name"] in args.schedds.split(",")]

    return schedd_ads


def get_startds(args=None):
    """
    Return a list of startd ads representing all the startds in the pool.
    """
    collectors = args.collectors
    if collectors:
        collectors = collectors.split(',')
    else:
        collectors = []

    startd_ads = {}
    for host in collectors:
        coll = htcondor.Collector(host)
        try:
            # get one ad per machine
            name_ads = coll.query(htcondor.AdTypes.Startd,
                                      constraint = '(SlotType == "Static") || (SlotType == "Partitionable")',
                                      projection = ["Name"])
            for ad in name_ads:
                try:
                    if ad["Name"][0:6] == "slot1@":
                        startd = coll.locate(htcondor.DaemonTypes.Startd, ad["Name"])
                        startd["MyPool"] = host
                        startd_ads[startd["Machine"]] = startd
                except Exception:
                    continue

        except IOError as e:
            logging.warning(str(e))
            continue

    startd_ads = list(startd_ads.values())
    random.shuffle(startd_ads)

    if args and args.startds:
        return [s for s in startd_ads if s["Machine"] in args.startds.split(",")]

    return startd_ads


def send_email_alert(recipients, subject, message):
    """
    Send a simple email alert (typically of failure).
    """
    if not recipients:
        return
    msg = email.mime.text.MIMEText(message)
    msg[
        "Subject"
    ] = f"{socket.gethostname()} - {time.strftime('%b %d, %H:%M')}h: {subject}"

    domain = socket.getfqdn()
    uid = os.geteuid()
    pw_info = pwd.getpwuid(uid)
    if "cern.ch" not in domain:
        domain = "%s.unl.edu" % socket.gethostname()
    msg["From"] = f"{pw_info.pw_name}@{domain}"
    msg["To"] = recipients[0]

    try:
        sess = smtplib.SMTP("localhost")
        sess.sendmail(msg["From"], recipients, msg.as_string())
        sess.quit()
    except Exception as exn:  # pylint: disable=broad-except
        logging.warning("Email notification failed: %s", str(exn))


def time_remaining(starttime, timeout=TIMEOUT_MINS * 60, positive=True):
    """
    Return the remaining time (in seconds) until starttime + timeout
    Returns 0 if there is no time remaining
    """
    elapsed = time.time() - starttime
    if positive:
        return max(0, timeout - elapsed)
    return timeout - elapsed


def set_up_logging(args):
    """Configure root logger with rotating file handler"""
    logger = logging.getLogger()

    log_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(log_level, int):
        raise ValueError("Invalid log level: %s" % log_level)
    logger.setLevel(log_level)

    if log_level <= logging.INFO:
        logging.getLogger("stomp.py").setLevel(log_level + 10)

    try:
        os.makedirs(args.log_dir)
    except OSError as oserr:
        if oserr.errno != errno.EEXIST:
            raise

    log_file = os.path.join(args.log_dir, "spider.log")
    filehandler = logging.handlers.RotatingFileHandler(log_file, maxBytes=100000)
    filehandler.setFormatter(
        logging.Formatter("%(asctime)s : %(name)s:%(levelname)s - %(message)s")
    )
    logger.addHandler(filehandler)

    if os.isatty(sys.stdout.fileno()):
        streamhandler = logging.StreamHandler(stream=sys.stdout)
        logger.addHandler(streamhandler)


def collect_metadata():
    """
    Return a dictionary with:
    - hostname
    - username
    - current time (in epoch millisec)
    """
    result = {}
    result["spider_hostname"] = socket.gethostname()
    result["spider_username"] = pwd.getpwuid(os.geteuid()).pw_name
    result["spider_runtime"] = int(time.time() * 1000)
    return result
