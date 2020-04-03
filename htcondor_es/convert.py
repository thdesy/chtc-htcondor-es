#!/usr/bin/python

import re
import json
import time
import logging
import zlib
import base64

import classad

# TEXT_ATTRS should only contain attrs that we want full text search on,
# otherwise strings are stored as keywords.
TEXT_ATTRS = {}

INDEXED_KEYWORD_ATTRS = {
    "Status",
    "Universe",
    "CondorPlatform",
    "CondorVersion",
    "ShouldTransferFiles",
    "WhenToTransferOutput",
    "TargetType",
    "MyType",
    "AutoClusterId",
    "AccountingGroup",
    "DockerImage",
    "GlobalJobId",
    "GridJobId",
    "LastRemoteHost",
    "LastRemotePool",
    "RemoteHost",
    "StartdSlot",
    "StartdName",
    "Owner",
    "User",
    "WMAgent_AgentName",
    "WMAgent_RequestName",
    "WMAgent_SubTaskName",
    "x509UserProxyEmail",
    "x509UserProxyFirstFQAN",
    "x509UserProxyFQAN",
    "x509userproxysubject",
    "x509UserProxyVOName",
    "DAGNodeName",
    "DAGParentNodeNames",
    "ScheddName",
    "GlideinClient",
    "GlideinEntryName",
    "GlideinFactory",
    "GlideinFrontendName",
    "GlideinName",
    "GLIDEIN_Entry_Name",
    "MATCH_EXP_JOB_GLIDECLIENT_Name",
    "MATCH_EXP_JOB_GLIDEIN_ClusterId",
    "MATCH_EXP_JOB_GLIDEIN_Entry_Name",
    "MATCH_EXP_JOB_GLIDEIN_Factory",
    "MATCH_EXP_JOB_GLIDEIN_Name",
    "MATCH_EXP_JOB_GLIDEIN_Schedd",
    "MATCH_EXP_JOB_GLIDEIN_SEs",
    "MATCH_EXP_JOB_GLIDEIN_Site",
    "MATCH_EXP_JOB_GLIDEIN_SiteWMS",
    "MATCH_EXP_JOB_GLIDEIN_SiteWMS_JobId",
    "MATCH_EXP_JOB_GLIDEIN_SiteWMS_Queue",
    "MATCH_EXP_JOB_GLIDEIN_SiteWMS_Slot",
    "JobState",
    "JobBatchName",
}

NOINDEX_KEYWORD_ATTRS = {
    "Cmd",
    "Args",
    "Arguments",
    "In",
    "Out",
    "Err",
    "Iwd",
    "UserLog",
    "RootDir",
    "TransferInput",
    "TransferOutput",
    "TransferOutputRemaps",
    "SpooledOutputFiles",
    "ExitReason",
    "LastHoldReason",
    "ReleaseReason",
    "RemoveReason",
    "LastRejMatchReason",
    "SubmitEventNotes",
    "StartdIpAddr",
    "StartdPrincipal",
    "StarterIpAddr",
    "StarterPrincipal",
    "DAGManNodesLog",
    "DAGManNodesMask",
}

FLOAT_ATTRS = {
    "Rank",
    "CPUsUsage",
    "QueueHr",
    "WallClockHr",
    "CommittedCoreHr",
    "CoreHr",
    "CpuTimeHr",
    "BadputHr",
    "CpuBadputHr",
    "CpuGoodputHr",
    "GpuBadputHr",
    "GpuGoodputHr",
    "MemoryMB",
    "DiskUsageGB",
    "DataRecvdMB",
    "DataSentMB",
    "JobDuration",
}

INT_ATTRS = {
    "AutoClusterId",
    "BytesRecvd",
    "BytesSent",
    "ClusterId",
    "CommittedSlotTime",
    "CpusProvisioned",
    "CumulativeSlotTime",
    "CumulativeSuspensionTime",
    "CurrentHosts",
    "DAGManJobId",
    "DelegatedProxyExpiration",
    "DiskProvisioned",
    "DiskUsage",
    "DiskUsage_RAW",
    "ExecutableSize",
    "ExecutableSize_RAW",
    "ExitStatus",
    "GpusProvisioned",
    "ImageSize",
    "ImageSize_RAW",
    "JobLeaseDuration",
    "JobPrio",
    "JobRunCount",
    "JobStatus",
    "JobFailed",
    "JobUniverse",
    "LastJobStatus",
    "LocalSysCpu",
    "LocalUserCpu",
    "MachineAttrCpus0",
    "MachineAttrSlotWeight0",
    "MaxHosts",
    "MaxWallTimeMins",
    "MaxWallTimeMins_RAW",
    "MemoryProvisioned",
    "MemoryUsage",
    "MinHosts",
    "NumCkpts",
    "NumCkpts_RAW",
    "NumJobMatches",
    "NumJobStarts",
    "NumRestarts",
    "NumShadowStarts",
    "NumSystemHolds",
    "OrigMaxHosts",
    "PilotRestLifeTimeMins",
    "PostJobPrio1",
    "PostJobPrio2",
    "ProcId",
    "RecentBlockReadKbytes",
    "RecentBlockReads",
    "RecentBlockWriteKbytes",
    "RecentBlockWrites",
    "RecentStatsLifetimeStarter",
    "RemoteSlotID",
    "RemoteSysCpu",
    "RemoteUserCpu",
    "RemoteWallClockTime",
    "RequestCpus",
    "RequestGpus",
    "RequestDisk",
    "RequestDisk_RAW",
    "RequestMemory",
    "RequestMemory_RAW",
    "ResidentSetSize",
    "ResidentSetSize_RAW",
    "StatsLifetimeStarter",
    "TotalSuspensions",
    "TransferInputSizeMB",
    "WallClockCheckpoint",
    "WMAgent_JobID",
    "DataLocationsCount",
    "MATCH_EXP_JOB_GLIDEIN_Job_Max_Time",
    "MATCH_EXP_JOB_GLIDEIN_MaxMemMBs",
    "MATCH_EXP_JOB_GLIDEIN_Max_Walltime",
    "MATCH_EXP_JOB_GLIDEIN_Memory",
    "MATCH_EXP_JOB_GLIDEIN_ProcId",
    "MATCH_EXP_JOB_GLIDEIN_ToDie",
    "MATCH_EXP_JOB_GLIDEIN_ToRetire",
}

DATE_ATTRS = {
    "CompletionDate",
    "EnteredCurrentStatus",
    "JobCurrentStartDate",
    "JobCurrentStartExecutingDate",
    "JobCurrentStartTransferInputDate",
    "JobCurrentStartTransferOutputDate",
    "JobLastStartDate",
    "JobStartDate",
    "JobCurrentFinishTransferInputDate",
    "JobCurrentFinishTransferOutputDate",
    "TransferInStarted",
    "TransferInFinished",
    "TransferOutFinished",
    "TransferOutStarted",
    "LastMatchTime",
    "LastSuspensionTime",
    "LastVacateTime",
    "LastVacateTime_RAW",
    "MATCH_GLIDEIN_ToDie",
    "MATCH_GLIDEIN_ToRetire",
    "QDate",
    "ShadowBday",
    "StageInFinish",
    "StageInStart",
    "JobFinishedHookDone",
    "LastJobLeaseRenewal",
    "LastRemoteStatusUpdate",
    "GLIDEIN_ToDie",
    "GLIDEIN_ToRetire",
    "DataCollectionDate",
    "RecordTime",
}

BOOL_ATTRS = {
    "CurrentStatusUnknown",
    "GlobusResubmit",
    "TransferIn",
    "TransferQueued",
    "TransferringInput",
    "NiceUser",
    "ExitBySignal",
    "HasBeenRouted",
    "HasBeenOverflowRouted",
    "HasBeenTimingTuned",
    "OnExitHold",
    "OnExitRemove",
    "PeriodicHold",
    "PeriodicRelease",
    "PeriodicRemove",
    "StreamErr",
    "StreamOut",
    "UserLogUseXML",
    "use_x509userproxy",
    "WantCheckpoint",
    "WantRemoteIO",
    "WantRemoteSyscalls",
    "LeaveJobInQueue",
}

IGNORE_ATTRS = {
    "CmdHash",
    "Environment",
    "EnvDelim",
    "Env",
    "ExecutableSize",
    "GlideinCredentialIdentifier",
    "GlideinSecurityClass",
    "JobNotification",
    "PublicClaimId",
    "LastPublicClaimId",
    "orig_environment",
    "osg_environment",
    "ClaimId",
}

# Fields to be kept in docs concerning running jobs
RUNNING_FIELDS = {
    "AccountingGroup",
    "AutoClusterId",
    "AffiliationInstitute",
    "AffiliationCountry",
    "BenchmarkJobDB12",
    "CommittedCoreHr",
    "CommittedTime",
    "CoreHr",
    "Country",
    "CpuBadput",
    "CpuEff",
    "CpuEventRate",
    "CpuTimeHr",
    "CpuTimePerEvent",
    "EnteredCurrentStatus",
    "EventRate",
    "GlobalJobId",
    "GLIDEIN_Entry_Name",
    "InputData",
    "InputGB",
    "JobPrio",
    "JobCurrentStartDate",
    "JobLastStartDate",
    "KEvents",
    "MegaEvents",
    "MemoryMB",
    "OutputGB",
    "QueueHr",
    "QDate",
    "ReadTimeMins",
    "RecordTime",
    "RemoteHost",
    "RequestCpus",
    "RequestMemory",
    "ScheddName",
    "Site",
    "Status",
    "TaskType",
    "Tier",
    "TimePerEvent",
    "Type",
    "WallClockHr",
    "WMAgent_JobID",
    "WMAgent_RequestName",
    "WMAgent_SubTaskName",
    "Workflow",
    "EstimatedWallTimeMins",
    "EstimatedWallTimeJobCount",
    "PilotRestLifeTimeMins",
    "LastRouted",
    "LastTimingTuned",
    "LPCRouted",
    "MemoryUsage",
    "PeriodicHoldReason",
    "RouteType",
    "HasBeenOverflowRouted",
    "HasBeenRouted",
    "HasBeenTimingTuned",
}

STATUS = {
    0: "Unexpanded",
    1: "Idle",
    2: "Running",
    3: "Removed",
    4: "Completed",
    5: "Held",
    6: "Error",
}

UNIVERSE = {
    1: "Standard",
    2: "Pipe",
    3: "Linda",
    4: "PVM",
    5: "Vanilla",
    6: "PVMD",
    7: "Scheduler",
    8: "MPI",
    9: "Grid",
    10: "Java",
    11: "Parallel",
    12: "Local",
}

POSTJOB_STATUS_DECODE = {
    "NOT RUN": "postProc",
    "TRANSFERRING": "transferring",
    "COOLOFF": "toRetry",
    "FAILED": "failed",
    "FINISHED": "finished",
}

_LAUNCH_TIME = int(time.time())


def ad_pop(ad, key):
    v = ad[key]
    del ad[key]
    return v


def make_list_from_string_field(ad, key, split_re=r"[\s,]+\s*", default=None):
    default = default or ["UNKNOWN"]
    try:
        return re.split(split_re, ad[key])
    except (TypeError, KeyError):
        return default


CREAM_RE = re.compile(r"CPUNumber = (\d+)")
NORDUGRID_RE = re.compile(r"\(count=(\d+)\)")
# Executable error messages in WMCore
WMCORE_EXE_EXMSG_RE = re.compile(r"^Chirp_WMCore_[A-Za-z0-9]+_Exception_Message$")


def to_json(ad, return_dict=False, reduce_data=False):
    if ad.get("TaskType") == "ROOT":
        return None
    result = {}

    result["RecordTime"] = record_time(ad)
    result["DataCollection"] = ad.get("CompletionDate", 0) or _LAUNCH_TIME
    result["DataCollectionDate"] = result["RecordTime"]

    result["ScheddName"] = ad.get("GlobalJobId", "UNKNOWN").split("#")[0]
    result["StartdSlot"] = ad.get("RemoteHost",
                                ad.get("LastRemoteHost", "UNKNOWN@UNKNOWN")).split("@")[0]
    result["StartdName"] = ad.get("RemoteHost",
                                ad.get("LastRemoteHost", "UNKNOWN@UNKNOWN")).split("@")[-1]

    # Enforce camel case names for GPU attrs
    if "RequestGpus" in ad:
        ad["RequestGpus"] = ad_pop(ad, "RequestGpus")
    if "GpusProvisioned" in ad:
        ad["GpusProvisioned"] = ad_pop(ad, "GpusProvisioned")

    bulk_convert_ad_data(ad, result)

    # Classify failed jobs
    result["JobFailed"] = job_failed(ad)
    result["ExitCode"] = common_exit_code(ad)
    if "ExitCode" in ad:
        result["CondorExitCode"] = ad["ExitCode"]

    now = time.time()
    if ad.get("JobStatus") == 2 and (ad.get("EnteredCurrentStatus", now + 1) < now):
        ad["RemoteWallClockTime"] = int(now - ad["EnteredCurrentStatus"])
        ad["CommittedTime"] = ad["RemoteWallClockTime"]
    result["WallClockHr"] = ad.get("RemoteWallClockTime", 0) / 3600

    result["DataRecvdMB"] = ad.get("BytesRecvd", 0) / 1e6
    result["DataSentMB"] = ad.get("BytesSent", 0) / 1e6
    
    slot_cpus = []
    if "RequestCpus" not in ad:
        m = CREAM_RE.search(ad.get("CreamAttributes", ""))
        m2 = NORDUGRID_RE.search(ad.get("NordugridRSL"))
        if m:
            try:
                ad["RequestCpus"] = int(m.groups()[0])
            except ValueError:
                pass
        elif m2:
            try:
                ad["RequestCpus"] = int(m2.groups()[0])
            except ValueError:
                pass
        elif "xcount" in ad:
            ad["RequestCpus"] = ad["xcount"]
    elif not isinstance(ad.eval("RequestCpus"), classad.Value):
        slot_cpus.append(int(ad.eval("RequestCpus")))
    if "CpusProvisioned" in ad and not isinstance(ad.eval("CpusProvisioned"), classad.Value):
        slot_cpus.append(int(ad.eval("CpusProvisioned")))
    if len(slot_cpus) > 0:
        slot_cpus = max(min(slot_cpus), 1) # assume used CPUs is minimum of requested or provided
    else:
        slot_cpus = 1 # assume job had to use at least one CPU
    result["CoreHr"] = (
        slot_cpus * int(ad.get("RemoteWallClockTime", 0)) / 3600
    )
    result["CommittedCoreHr"] = (
        slot_cpus * ad.get("CommittedTime", 0) / 3600
    )
    result["CommittedWallClockHr"] = ad.get("CommittedTime", 0) / 3600
    result["CpuTimeHr"] = (
        ad.get("RemoteSysCpu", 0) + ad.get("RemoteUserCpu", 0)
    ) / 3600.0

    result["DiskUsageGB"] = ad.get("DiskUsage_RAW", 0) / 1000000

    result["MemoryMB"] = ad.get("ResidentSetSize_RAW", 0) / 1024

    slot_gpus = []
    if "RequestGpus" in ad:
        if not isinstance(ad.eval("RequestGpus"), classad.Value):
            slot_gpus.append(int(ad.eval("RequestGpus")))
    if "GpusProvisioned" in ad:
        if (
                not isinstance(ad.eval("GpusProvisioned"), classad.Value) and
                not (len(slot_gpus) == 1 and slot_gpus[0] == 0)
            ):
            slot_gpus.append(int(ad.eval("GpusProvisioned")))
    # only compute GPU stats if at least one GPU was requested
    if "RequestGpus" in ad and len(slot_gpus) > 0 and slot_gpus[0] != 0:
        slot_gpus = min(slot_gpus) # assume used GPUs is minimum of request or provided
        result["GpuCoreHr"] = (
            slot_gpus * int(ad.get("RemoteWallClockTime", 0)) / 3600
        )
        result["CommittedGpuCoreHr"] = (
            slot_gpus * ad.get("CommittedTime", 0) / 3600
        )
        result["GpuBadputHr"] = max(result["GpuCoreHr"] - result["CommittedGpuCoreHr"], 0)
        result["GpuGoodputHr"] = max(result["GpuCoreHr"] - result["GpuBadputHr"], 0)

    if "x509UserProxyFQAN" in ad:
        result["x509UserProxyFQAN"] = str(ad["x509UserProxyFQAN"]).split(",")
    if "x509UserProxyVOName" in ad:
        result["VO"] = str(ad["x509UserProxyVOName"])
    elif ("GlideinEntryName" in ad) and ("MATCH_EXP_JOBGLIDEIN_ResourceName" not in ad):
        result["Site"] = ad["GlideinEntryName"]
    else:
        result["Site"] = ad.get("MATCH_EXP_JOBGLIDEIN_ResourceName", "UNKNOWN")

    if result["WallClockHr"] == 0:
        result["CpuEff"] = 0
    else:
        result["CpuEff"] = (
            100
            * result["CpuTimeHr"]
            / result["WallClockHr"]
            / slot_cpus
        )
    result["Status"] = STATUS.get(ad.get("JobStatus"), "Unknown")
    result["Universe"] = UNIVERSE.get(ad.get("JobUniverse"), "Unknown")
    result["QueueHr"] = (
        ad.get("JobCurrentStartDate", time.time()) - ad["QDate"]
    ) / 3600
    result["BadputHr"] = max(result["CoreHr"] - result["CommittedCoreHr"], 0)
    result["CpuBadputHr"] = max(result["CoreHr"] - result["CpuTimeHr"], 0)
    result["CpuGoodputHr"] = max(result["CoreHr"] - result["CpuBadputHr"], 0)

    # Parse new machine statistics.
    try:
        cpus = float(result["GLIDEIN_Cpus"])
        result["BenchmarkJobHS06"] = float(ad["MachineAttrMJF_JOB_HS06_JOB0"]) / cpus
        if result.get("EventRate", 0) > 0:
            result["HS06EventRate"] = result["EventRate"] / result["BenchmarkJobHS06"]
        if result.get("CpuEventRate", 0) > 0:
            result["HS06CpuEventRate"] = (
                result["CpuEventRate"] / result["BenchmarkJobHS06"]
            )
        if result.get("CpuTimePerEvent", 0) > 0:
            result["HS06CpuTimePerEvent"] = (
                result["CpuTimePerEvent"] * result["BenchmarkJobHS06"]
            )
        if result.get("TimePerEvent", 0) > 0:
            result["HS06TimePerEvent"] = (
                result["TimePerEvent"] * result["BenchmarkJobHS06"]
            )
        result["HS06CoreHr"] = result["CoreHr"] * result["BenchmarkJobHS06"]
        result["HS06CommittedCoreHr"] = (
            result["CommittedCoreHr"] * result["BenchmarkJobHS06"]
        )
        result["HS06CpuTimeHr"] = result["CpuTimeHr"] * result["BenchmarkJobHS06"]
    except:
        result.pop("MachineAttrMJF_JOB_HS06_JOB0", None)

    if ("MachineAttrDIRACBenchmark0" in ad) and classad.ExprTree(
        "MachineAttrDIRACBenchmark0 isnt undefined"
    ).eval(ad):
        result["BenchmarkJobDB12"] = float(ad["MachineAttrDIRACBenchmark0"])
        if result.get("EventRate", 0) > 0:
            result["DB12EventRate"] = result["EventRate"] / result["BenchmarkJobDB12"]
        if result.get("CpuEventRate", 0) > 0:
            result["DB12CpuEventRate"] = (
                result["CpuEventRate"] / result["BenchmarkJobDB12"]
            )
        if result.get("CpuTimePerEvent", 0) > 0:
            result["DB12CpuTimePerEvent"] = (
                result["CpuTimePerEvent"] * result["BenchmarkJobDB12"]
            )
        if result.get("TimePerEvent", 0) > 0:
            result["DB12TimePerEvent"] = (
                result["TimePerEvent"] * result["BenchmarkJobDB12"]
            )
        result["DB12CoreHr"] = result["CoreHr"] * result["BenchmarkJobDB12"]
        result["DB12CommittedCoreHr"] = (
            result["CommittedCoreHr"] * result["BenchmarkJobDB12"]
        )
        result["DB12CpuTimeHr"] = result["CpuTimeHr"] * result["BenchmarkJobDB12"]

    if "MachineAttrCPUModel0" in ad:
        result["CPUModel"] = str(ad["MachineAttrCPUModel0"])
        result["CPUModelName"] = str(ad["MachineAttrCPUModel0"])
        result["Processor"] = str(ad["MachineAttrCPUModel0"])

    if reduce_data:
        result = drop_fields_for_running_jobs(result)

    if return_dict:
        return result
    else:
        return json.dumps(result)


def record_time(ad):
    """
    RecordTime falls back to launch time as last-resort and for jobs in the queue

    For Completed/Removed/Error jobs, try to update it:
        - to CompletionDate if present
        - else to EnteredCurrentStatus if present
        - else fall back to launch time
    """
    if ad["JobStatus"] in [3, 4, 6]:
        if ad.get("CompletionDate", 0) > 0:
            return ad["CompletionDate"]

        elif ad.get("EnteredCurrentStatus", 0) > 0:
            return ad["EnteredCurrentStatus"]

    return _LAUNCH_TIME


def job_failed(ad):
    """
    Returns 0 when none of the exitcode fields has a non-zero value
    otherwise returns 1
    """
    ec_fields = ["ExitCode"]

    if sum([ad.get(k, 0) for k in ec_fields]) > 0:
        return 1
    return 0


def common_exit_code(ad):
    """
    Consolidate the exit code values of JobExitCode
    and the original condor exit code.
    """
    return ad.get("JobExitCode", ad.get("ExitCode", 0))


_CONVERT_COUNT = 0
_CONVERT_CPU = 0


def bulk_convert_ad_data(ad, result):
    """
    Given a ClassAd, bulk convert to a python dictionary.
    """
    _keys = set(ad.keys()) - IGNORE_ATTRS
    for key in _keys:
        try:
            value = ad.eval(key)
        except:
            continue

        if isinstance(value, classad.Value):
            if (value is classad.Value.Error) or (value is classad.Value.Undefined):
                # Could not evaluate expression, store raw expression
                value = str(ad.get(key))
                key = key + "_EXPR"
            else:
                value = None
        elif (key in TEXT_ATTRS) or (key in INDEXED_KEYWORD_ATTRS) or (key in NOINDEX_KEYWORD_ATTRS):
            value = str(value)
        elif key in FLOAT_ATTRS:
            try:
                value = float(value)
            except ValueError:
                if isinstance(value, str) and value.lower() == "unknown":
                    value = None
                else:
                    logging.warning(
                        f"Failed to convert key {key} with value {repr(value)} to float"
                    )
                    continue
        elif key in INT_ATTRS:
            try:
                value = int(value)
            except ValueError:
                if isinstance(value, str) and value.lower() == "unknown":
                    value = None
                else:
                    logging.warning(
                        f"Failed to convert key {key} with value {repr(value)} to int"
                    )
                    continue
        elif key in BOOL_ATTRS:
            value = bool(value)
        elif key in DATE_ATTRS:
            if value == 0 or (isinstance(value, str) and value.lower() == "unknown"):
                value = None
            else:
                try:
                    value = int(value)
                except ValueError:
                    logging.warning(
                        "Failed to convert key %s with value %s to int for a date field"
                        % (key, repr(value))
                    )
                    value = None

        if WMCORE_EXE_EXMSG_RE.match(key):
            value = str(decode_and_decompress(value))

        result[key] = value


def decode_and_decompress(value):
    try:
        return str(zlib.decompress(base64.b64decode(value)))
    except (TypeError, zlib.error):
        logging.warning(f"Failed to decode and decompress value: {repr(value)}")


def convert_dates_to_millisecs(record):
    for date_field in DATE_ATTRS:
        try:
            record[date_field] *= 1000
        except (KeyError, TypeError):
            continue

    return record


def drop_fields_for_running_jobs(record):
    """
    Check if the job is running or pending
    and prune it if it is.
    """
    if "Status" in record and record["Status"] not in ["Running", "Idle", "Held"]:
        return record
    _fields = RUNNING_FIELDS.intersection(set(record.keys()))
    skimmed_record = {field: record[field] for field in _fields}
    return skimmed_record


def unique_doc_id(doc):
    """
    Return a string of format "<GlobalJobId>#<RecordTime>"
    To uniquely identify documents (not jobs)

    Note that this uniqueness breaks if the same jobs are submitted
    with the same RecordTime
    """
    return "%s#%d" % (doc["GlobalJobId"], doc["RecordTime"])
