#!/usr/bin/python

import re
import json
import time
import logging
import zlib
import base64

import classad

STRING_VALS = {
    "AutoClusterId",
    "AffiliationInstitute",
    "AffiliationCountry",
    "Processor",
    "ChirpCMSSWCPUModels",
    "CPUModel",
    "CPUModelName",
    "ChirpCMSSWCPUModels",
    "CMSPrimaryPrimaryDataset",
    "CMSPrimaryProcessedDataset",
    "CMSPrimaryDataTier",
    "CMSSWVersion",
    "CMSSWMajorVersion",
    "CMSSWReleaseSeries",
    "CRAB_JobType",
    "CRAB_JobSW",
    "CRAB_JobArch",
    "CRAB_Id",
    "CRAB_ISB",
    "CRAB_PostJobStatus",
    "CRAB_Workflow",
    "CRAB_UserRole",
    "CMSGroups",
    "CRAB_UserHN",
    "CRAB_UserGroup",
    "CRAB_TaskWorker",
    "CRAB_SiteWhitelist",
    "CRAB_SiteBlacklist",
    "CRAB_SplitAlgo",
    "CRAB_PrimaryDataset",
    "Args",
    "AccountingGroup",
    "Cmd",
    "CMS_JobType",
    "CMS_WMTool",
    "DESIRED_Archs",
    "DESIRED_CMSDataLocations",
    "DESIRED_CMSDataset",
    "DESIRED_Sites",
    "ExtDESIRED_Sites",
    "FormattedCrabId",
    "GlobalJobId",
    "GlideinClient",
    "GlideinEntryName",
    "GlideinFactory",
    "GlideinFrontendName",
    "GlideinName",
    "GLIDEIN_Entry_Name",
    "GlobusRSL",
    "GridJobId",
    "LastRemoteHost",
    "MATCH_EXP_JOB_GLIDECLIENT_Name",
    "MATCH_EXP_JOB_GLIDEIN_ClusterId",
    "MATCH_EXP_JOB_GLIDEIN_CMSSite",
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
    "Owner",
    "Rank",
    "RemoteHost",
    "REQUIRED_OS",
    "ShouldTransferFiles",
    "StartdIpAddr",
    "StartdPrincipal",
    "User",
    "WhenToTransferOutput",
    "WMAgent_AgentName",
    "WMAgent_RequestName",
    "WMAgent_SubTaskName",
    "x509UserProxyEmail",
    "x509UserProxyFirstFQAN",
    "x509UserProxyFQAN",
    "x509userproxysubject",
    "x509UserProxyVOName",
    "InputData",
    "Original_DESIRED_Sites",
    "WMAgent_TaskType",
    "NordugridRSL",
    "Campaign",
    "TaskType",
    "DataLocations",
    "Workflow",
    "Site",
    "Tier",
    "Country",
    "Status",
    "Universe",
    "ExitReason",
    "LastHoldReason",
    "RemoveReason",
    "DESIRED_Overflow_Region",
    "DESIRED_OpSysMajorVers",
    "DESIRED_CMSDataset",
    "DAGNodeName",
    "DAGParentNodeNames",
    "OverflowType",
    "ScheddName",
}

INT_VALS = {
    "CRAB_Retry",
    "BytesRecvd",
    "BytesSent",
    "ClusterId",
    "CommittedSlotTime",
    "CumulativeSlotTime",
    "CumulativeSuspensionTime",
    "CurrentHosts",
    "CRAB_JobCount",
    "DelegatedProxyExpiration",
    "DiskUsage_RAW",
    "ExecutableSize_RAW",
    "ExitStatus",
    "GlobusStatus",
    "ImageSize_RAW",
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
    "MATCH_EXP_JOB_GLIDEIN_Job_Max_Time",
    "MATCH_EXP_JOB_GLIDEIN_MaxMemMBs",
    "MATCH_EXP_JOB_GLIDEIN_Max_Walltime",
    "MATCH_EXP_JOB_GLIDEIN_Memory",
    "MATCH_EXP_JOB_GLIDEIN_ProcId",
    "MATCH_EXP_JOB_GLIDEIN_ToDie",
    "MATCH_EXP_JOB_GLIDEIN_ToRetire",
    "MaxHosts",
    "MaxWallTimeMins_RAW",
    "MemoryUsage",
    "MinHosts",
    "NumGlobusSubmits" "NumJobMatches",
    "NumJobStarts",
    "NumRestarts",
    "NumShadowStarts",
    "NumSystemHolds",
    "PilotRestLifeTimeMins",
    "PostJobPrio1",
    "PostJobPrio2",
    "ProcId",
    "RecentBlockReadKbytes",
    "RecentBlockReads",
    "RecentBlockWriteKbytes",
    "RecentBlockWrites",
    "RemoteSlotID",
    "RemoteSysCpu",
    "RemoteUserCpu",
    "RemoteWallClockTime",
    "RequestCpus",
    "RequestDisk_RAW",
    "RequestMemory_RAW",
    "ResidentSetSize_RAW",
    "StatsLifetimeStarter",
    "TotalSuspensions",
    "TransferInputSizeMB",
    "WallClockCheckpoint",
    "WMAgent_JobID",
    "DesiredSiteCount",
    "DataLocationsCount",
}

DATE_VALS = {
    "CompletionDate",
    "CRAB_TaskCreationDate",
    "EnteredCurrentStatus",
    "JobCurrentStartDate",
    "JobCurrentStartExecutingDate",
    "JobCurrentStartTransferOutputDate",
    "JobLastStartDate",
    "JobStartDate",
    "LastMatchTime",
    "LastSuspensionTime",
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
    "ChirpCMSSWLastUpdate",
}

BOOL_VALS = {
    "CurrentStatusUnknown",
    "CRAB_Publish",
    "CRAB_SaveLogsFlag",
    "CRAB_TransferOutputs",
    "GlobusResubmit",
    "TransferQueued",
    "TransferringInput",
    "HasSingularity",
    "NiceUser",
    "ExitBySignal",
    "CMSSWDone",
    "HasBeenRouted",
    "HasBeenOverflowRouted",
    "HasBeenTimingTuned",
}

IGNORE = {
    "Arguments",
    "CmdHash",
    "CRAB_UserDN",
    "CRAB_Destination",
    "CRAB_DBSURL",
    "CRAB_ASOURL",
    "CRAB_ASODB",
    "CRAB_AdditionalOutputFiles",
    "CRAB_EDMOutputFiles",
    "CRAB_TFileOutputFiles",
    "CRAB_oneEventMode",
    "CRAB_NumAutomJobRetries",
    "CRAB_localOutputFiles",
    "CRAB_ASOTimeout",
    "CRAB_OutTempLFNDir",
    "CRAB_PublishDBSURL",
    "CRAB_PublishGroupName",
    "CRAB_RestURInoAPI",
    "CRAB_RestHost",
    "CRAB_ReqName",
    "CRAB_RetryOnASOFailures",
    "CRAB_StageoutPolicy",
    "SubmitEventNotes",
    "DAGManNodesMask",
    "DAGManNodesLog",
    "DAGManJobId",
    "accounting_group",
    "AcctGroup",
    "AcctGroupUser",
    "AllowOpportunistic",
    "AutoClusterAttrs",
    "BufferBlockSize",
    "BufferSize",
    "CondorPlatform",
    "CondorVersion",
    "DiskUsage",
    "Err",
    "Environment",
    "EnvDelim",
    "Env",
    "ExecutableSize",
    "HasPrioCorrection",
    "GlideinCredentialIdentifier",
    "GlideinLogNr",
    "GlideinSecurityClass",
    "GlideinSlotsLayout",
    "GlideinWebBase",
    "GlideinWorkDir",
    "ImageSize",
    "In",
    "Iwd",
    "JobAdInformationAttrs",
    "job_ad_information_attrs",
    "JOB_GLIDECLIENT_Name",
    "JOB_GLIDEIN_ClusterId",
    "JOB_GLIDEIN_CMSSite",
    "JOBGLIDEIN_CMSSite",
    "JOB_GLIDEIN_Entry_Name",
    "JOB_GLIDEIN_Factory",
    "JOB_GLIDEIN_Job_Max_Time",
    "JOB_GLIDEIN_MaxMemMBs",
    "JOB_GLIDEIN_Max_Walltime",
    "JOB_GLIDEIN_Memory",
    "JOB_GLIDEIN_Name",
    "JOB_GLIDEIN_ProcId",
    "JOB_GLIDEIN_Schedd",
    "JOB_GLIDEIN_SEs",
    "JOB_GLIDEIN_Site",
    "JOB_GLIDEIN_SiteWMS",
    "JOB_GLIDEIN_SiteWMS_JobId",
    "JOB_GLIDEIN_SiteWMS_Queue",
    "JOB_GLIDEIN_SiteWMS_Slot",
    "JOB_GLIDEIN_ToDie",
    "JOB_GLIDEIN_ToRetire",
    "JobLeaseDuration",
    "JobNotification",
    "JOB_Site",
    "Managed",
    "MATCH_EXP_JOBGLIDEIN_CMSSite",
    "MATCH_EXP_JOB_Site",
    "MATCH_GLIDECLIENT_Name",
    "MATCH_GLIDEIN_ClusterId",
    "MATCH_GLIDEIN_CMSSite",
    "MATCH_GLIDEIN_Entry_Name",
    "MATCH_GLIDEIN_Factory",
    "MATCH_GLIDEIN_Job_Max_Time",
    "MATCH_GLIDEIN_MaxMemMBs",
    "MATCH_GLIDEIN_Max_Walltime",
    "MATCH_GLIDEIN_Name",
    "MATCH_GLIDEIN_ProcId",
    "MATCH_GLIDEIN_Schedd",
    "MATCH_GLIDEIN_SEs",
    "MATCH_GLIDEIN_Site",
    "MATCH_GLIDEIN_SiteWMS",
    "MATCH_GLIDEIN_SiteWMS_JobId",
    "MATCH_GLIDEIN_SiteWMS_Queue",
    "MATCH_GLIDEIN_SiteWMS_Slot",
    "MATCH_Memory",
    "MyType",
    "NiceUser",
    "NumCkpts",
    "NumCkpts_RAW",
    "OnExitHold",
    "OnExitRemove",
    "OrigMaxHosts",
    "Out",
    "PeriodicHold",
    "PeriodicRelease",
    "PeriodicRemove",
    "Prev_DESIRED_Sites",
    "PublicClaimId",
    "RequestDisk",
    "RequestMemory",
    "ResidentSetSize",
    "REQUIRES_LOCAL_DATA",
    "RecentBlockReadKbytes",
    "RecentBlockReads",
    "RecentBlockWriteKbytes",
    "RecentBlockWrites",
    "RootDir",
    "ServerTime",
    "SpooledOutputFiles",
    "StreamErr",
    "StreamOut",
    "TargetType",
    "TransferIn",
    "TransferInput",
    "TransferOutput",
    "UserLog",
    "UserLogUseXML",
    "use_x509userproxy",
    "x509userproxy",
    "x509UserProxyExpiration",
    "WantCheckpoint",
    "WantRemoteIO",
    "WantRemoteSyscalls",
    "BlockReadKbytes",
    "BlockReads",
    "BlockWriteKbytes",
    "BlockWrites",
    "LocalSysCpu",
    "LeaveJobInQueue",
    "LocalUserCpu",
    "JobMachineAttrs",
    "LastRejMatchReason",
    "MachineAttrGLIDEIN_CMSSite0",
    "CMS_ALLOW_OVERFLOW",
    "LastPublicClaimId",
    "LastRemotePool",
    "Used_Gatekeeper",
    "DESIRED_OpSyses",
}

NO_INDEX = {
    "CRAB_OutLFNDir",
    "Args",
    "Cmd",
    "BytesRecvd",
    "CoreSize",
    "DelegatedProxyExpiration",
    "Environment",
    "RecentBlockReadKbytes",
    "RecentBlockReads",
    "RecentBlockWriteKbytes",
    "RecentBlockWrites",
    "RecentStatsLifetimeStarter",
    "CurrentHosts",
    "MachineAttrCpus0",
    "MachineAttrSlotWeight0",
    "LocalSysCpu",
    "LocalUserCpu",
    "MaxHosts",
    "MinHosts",
    "StartdIpAddr",
    "StartdPrincipal",
    "LastRemoteHost",
}

NO_ANALYSIS = {
    "CRAB_PublishName",
    "CRAB_PublishGroupName",
    "CondorPlatform",
    "CondorVersion",
    "CurrentHosts",
    "DESIRED_Archs",
    "ShouldTransferFiles",
    "TotalSuspensions",
    "REQUIRED_OS",
    "ShouldTransferFiles",
    "WhenToTransferOutput",
    "DAGParentNodeNames",
}

# Fields to be kept in docs concerning running jobs
RUNNING_FIELDS = {
    "AccountingGroup",
    "AutoClusterId",
    "AffiliationInstitute",
    "AffiliationCountry",
    "BenchmarkJobDB12",
    "Campaign",
    "CMS_JobType",
    "CMS_JobRetryCount",
    "CMS_Pool",
    "CMSGroups",
    "CMSPrimaryDataTier",
    "CMSSWKLumis",
    "CMSSWWallHrs",
    "CMSSWVersion",
    "CMSSWMajorVersion",
    "CMSSWReleaseSeries",
    "CommittedCoreHr",
    "CommittedTime",
    "CoreHr",
    "Country",
    "CpuBadput",
    "CpuEff",
    "CpuEventRate",
    "CpuTimeHr",
    "CpuTimePerEvent",
    "CRAB_AsyncDest",
    "CRAB_DataBlock",
    "CRAB_Id",
    "CRAB_JobCount",
    "CRAB_PostJobStatus",
    "CRAB_Retry",
    "CRAB_TaskCreationDate",
    "CRAB_UserHN",
    "CRAB_Workflow",
    "CRAB_SplitAlgo",
    "CMS_SubmissionTool",
    "CMS_WMTool",
    "DESIRED_CMSDataset",
    "EnteredCurrentStatus",
    "EventRate",
    "FormattedCrabId",
    "GlobalJobId",
    "GLIDEIN_Entry_Name",
    "HasSingularity",
    "InputData",
    "InputGB",
    "JobPrio",
    "JobCurrentStartDate",
    "JobLastStartDate",
    "KEvents",
    "MegaEvents",
    "MemoryMB",
    "OutputGB",
    "QueueHrs",
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
    "DESIRED_Sites",
    "DESIRED_SITES_Diff",
    "DESIRED_SITES_Orig",
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


def make_list_from_string_field(ad, key, split_re=r"[\s,]+\s*", default=None):
    default = default or ["UNKNOWN"]
    try:
        return re.split(split_re, ad[key])
    except (TypeError, KeyError):
        return default


_cream_re = re.compile(r"CPUNumber = (\d+)")
_nordugrid_re = re.compile(r"\(count=(\d+)\)")
_camp_re = re.compile(r"[A-Za-z0-9_]+_[A-Z0-9]+-([A-Za-z0-9]+)-")
_prep_re = re.compile(r"[A-Za-z0-9_]+_([A-Z]+-([A-Za-z0-9]+)-[0-9]+)")
_rval_re = re.compile(r"[A-Za-z0-9]+_(RVCMSSW_[0-9]+_[0-9]+_[0-9]+)")
_prep_prompt_re = re.compile(r"(PromptReco|Repack|Express)_[A-Za-z0-9]+_([A-Za-z0-9]+)")
# Executable error messages in WMCore
_wmcore_exe_exmsg = re.compile(r"^Chirp_WMCore_[A-Za-z0-9]+_Exception_Message$")
# 2016 reRECO; of the form cerminar_Run2016B-v2-JetHT-23Sep2016_8020_160923_164036_4747
_rereco_re = re.compile(r"[A-Za-z0-9_]+_Run20[A-Za-z0-9-_]+-([A-Za-z0-9]+)")
_generic_site = re.compile(r"^[A-Za-z0-9]+_[A-Za-z0-9]+_(.*)_")
_cms_site = re.compile(r"CMS[A-Za-z]*_(.*)_")
_cmssw_version = re.compile(r"CMSSW_((\d*)_(\d*)_.*)")


def to_json(ad, return_dict=False, reduce_data=False):
    if ad.get("TaskType") == "ROOT":
        return None
    result = {}

    result["RecordTime"] = record_time(ad)
    result["DataCollection"] = ad.get("CompletionDate", 0) or _LAUNCH_TIME
    result["DataCollectionDate"] = result["RecordTime"]

    result["ScheddName"] = ad.get("GlobalJobId", "UNKNOWN").split("#")[0]

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

    if "RequestCpus" not in ad:
        m = _cream_re.search(ad.get("CreamAttributes", ""))
        m2 = _nordugrid_re.search(ad.get("NordugridRSL"))
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
    ad.setdefault("RequestCpus", 1)
    try:
        ad["RequestCpus"] = int(ad.eval("RequestCpus"))
    except ValueError:
        ad["RequestCpus"] = 1.0
    result["RequestCpus"] = ad["RequestCpus"]

    result["CoreHr"] = (
        ad.get("RequestCpus", 1.0) * int(ad.get("RemoteWallClockTime", 0)) / 3600
    )
    result["CommittedCoreHr"] = (
        ad.get("RequestCpus", 1.0) * ad.get("CommittedTime", 0) / 3600
    )
    result["CommittedWallClockHr"] = ad.get("CommittedTime", 0) / 3600
    result["CpuTimeHr"] = (
        ad.get("RemoteSysCpu", 0) + ad.get("RemoteUserCpu", 0)
    ) / 3600.0
    result["DiskUsageGB"] = ad.get("DiskUsage_RAW", 0) / 1000000
    result["MemoryMB"] = ad.get("ResidentSetSize_RAW", 0) / 1024

    if "x509UserProxyFQAN" in ad:
        result["x509UserProxyFQAN"] = str(ad["x509UserProxyFQAN"]).split(",")
    if "x509UserProxyVOName" in ad:
        result["VO"] = str(ad["x509UserProxyVOName"])
    elif ("GlideinEntryName" in ad) and ("MATCH_EXP_JOBGLIDEIN_ResourceName" not in ad):
        m = _generic_site.match(ad["GlideinEntryName"])
        m2 = _cms_site.match(ad["GlideinEntryName"])
        if m2:
            result["Site"] = m2.groups()[0]
            info = result["Site"].split("_", 2)
            if len(info) == 3:
                result["Tier"] = info[0]
                result["Country"] = info[1]
            else:
                result["Tier"] = "Unknown"
                result["Country"] = "Unknown"
        elif m:
            result["Site"] = m.groups()[0]
        else:
            result["Site"] = "UNKNOWN"
    else:
        result["Site"] = ad.get("MATCH_EXP_JOBGLIDEIN_ResourceName", "UNKNOWN")

    if result["WallClockHr"] == 0:
        result["CpuEff"] = 0
    else:
        result["CpuEff"] = (
            100
            * result["CpuTimeHr"]
            / result["WallClockHr"]
            / ad.get("RequestCpus", 1.0)
        )
    result["Status"] = STATUS.get(ad.get("JobStatus"), "Unknown")
    result["Universe"] = UNIVERSE.get(ad.get("JobUniverse"), "Unknown")
    result["QueueHrs"] = (
        ad.get("JobCurrentStartDate", time.time()) - ad["QDate"]
    ) / 3600
    result["Badput"] = max(result["CoreHr"] - result["CommittedCoreHr"], 0)
    result["CpuBadput"] = max(result["CoreHr"] - result["CpuTimeHr"], 0)

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

    result["HasSingularity"] = classad.ExprTree(
        "MachineAttrHAS_SINGULARITY0 is true"
    ).eval(ad)

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
    _keys = set(ad.keys()) - IGNORE
    for key in _keys:
        if key.startswith("HasBeen") and key not in BOOL_VALS:
            continue
        try:
            value = ad.eval(key)
        except:
            continue

        if isinstance(value, classad.Value):
            if value is classad.Value.Error:
                continue
            else:
                value = None
        elif key in BOOL_VALS:
            value = bool(value)
        elif key in INT_VALS:
            try:
                value = int(value)
            except ValueError:
                if value == "Unknown":
                    value = None
                else:
                    logging.warning(
                        f"Failed to convert key {key} with value {repr(value)} to int"
                    )
                    continue
        elif key in STRING_VALS:
            value = str(value)
        elif key in DATE_VALS:
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

        # elif key in date_vals:
        #    value = datetime.datetime.fromtimestamp(value).strftime("%Y-%m-%d %H:%M:%S")
        if key.startswith("MATCH_EXP_JOB_"):
            key = key[len("MATCH_EXP_JOB_") :]
        if key.endswith("_RAW"):
            key = key[: -len("_RAW")]
        if _wmcore_exe_exmsg.match(key):
            value = str(decode_and_decompress(value))

        result[key] = value


def decode_and_decompress(value):
    try:
        return str(zlib.decompress(base64.b64decode(value)))
    except (TypeError, zlib.error):
        logging.warning(f"Failed to decode and decompress value: {repr(value)}")


def convert_dates_to_millisecs(record):
    for date_field in DATE_VALS:
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
