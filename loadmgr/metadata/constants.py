"""
Created on Aug 12, 2014

@author: jwd3

Version History:
0.01 jwd 09/02/2014
    Added JOB_FILE_HEADER and LOG_FILE_VARIABLE
    Updated JOB_STEP_ATTRIBUTE_COUNT
0.02 jwd 09/10/2014
    Added event constants
    Added new job status JOB_STATUS_WAITING
    Update default configuration format
0.03 jwd 09/24/2014
    Added constants for env config (ENV_CONFIG_ENV_VAR,DEFAULT_ENV_CONFIG_FILENAME)
0.04 jwd 10/20/2014
    Added constant TASK_STATUS_WARNING
0.05 jwd 12/01/2014
    No changes
0.06 jwd 1/21/2015
    Changed CONFIG_ENV_VAR to "LOADMGR_CONFIG"
    Added constants for process management (load strategy, process type, log action, process status)
    Update default config
    Removed JOB_FILE_HEADER and JOB_STEP_ATTRIBUTE_COUNT
0.07 jwd 03/06/2015
    Converted version from float to string
    Update default config for new email lists for success and failure
    Added email_message_text and email_message_html for warning email that job is running
0.08 jwd 6/25/2015
    Added new load strategy chunk
0.09 jwd 8/11/2015
    Removed DEFAULT_CONFIG; deprecated use of default config.  Base config is required!!
0.10 jwd 8/26/2015
    Added load strategy FILE for file-based source ingestion
    Added METRICS_KW and FILE_TYPE_KW
0.11 jwd 11/24/2015
    Added END_DATE_VARIABLE
    Added PHG API Constants
    Added static file support
0.12 jwd 01/12/2016
    Added JOB_FILE_CSV_STEP_SCHEMA
0.13 jwd3 02/20/2017
    Updated PKGNAME
0.14 jwd3 05/14/2017 v2.0.0
    Changed version numbers to 0.xx where xx is a sequential number - major/minor/revisions not needed
    Added default config
    Added DEFAULT_LOW_DATE and DEFAULT_HIGH_DATE
0.15 jwd3 05/19/2017 v2.0.1
    Added HDFS_STAGE_MOVE_TEMPLATE constant
0.16 jwd3 05/22/2017 v2.1.2
    Changed name of GENERIC_LOADER
"""
import os

__version__ = "0.16"
__date__ = '2014-08-12'
__updated__ = '05/24/2017'

CONFIG_ENV_VAR = "LOADMGR_CONFIG"
DEFAULT_CONFIG_FILENAME = 'loadmgr_config.cfg'
#ENV_CONFIG_ENV_VAR = "LOADMGR_ENV_CONFIG"
#DEFAULT_ENV_CONFIG_FILENAME = 'loadmgr_env.cfg'
LOGGING_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
JOB_STATUS_READY = 'Ready'
JOB_STATUS_RUNNING = 'Running'
JOB_STATUS_COMPLETED = 'Completed'
JOB_STATUS_FAILED = 'Failed'
JOB_STATUS_NOT_RUNNING = 'Not Running'
JOB_STATUS_STOPPED = 'Stopped'
JOB_STATUS_SIP = 'Stopping In Progress'
JOB_STATUS_PAUSED = 'Paused'
JOB_STATUS_PIP = 'Pausing In Progress'
JOB_STATUS_WAITING = 'Waiting'
TASK_STATUS_SUCCESS = 'Success'
TASK_STATUS_FAILURE = 'Failure'
TASK_STATUS_WARNING = 'Warning'
ADMINLIST = ['jwd3']
GENERIC_LOADER = 'lmgrworker.py'
LOG_FILE_VARIABLE = 'job_log_file'
START_DATE_VARIABLE = 'currency_start_dttm'
END_DATE_VARIABLE = 'currency_end_dttm'
EVENT_ACTION_CREATE = 'create'
EVENT_ACTION_WAIT = 'wait'
EVENT_ACTION_TYPE = [EVENT_ACTION_WAIT,EVENT_ACTION_CREATE]
EVENT_TYPE_FILE = 'file'
EVENT_TYPE_OTHER = 'other'
EVENT_TYPE = [EVENT_TYPE_FILE,EVENT_TYPE_OTHER]
CALL_CREATE_EVENT = 'call_create_event'

# Valid values for process type field
PROCESS_TYPE_INGESTION = 'ingestion'
PROCESS_TYPE_CONSUMPTION = 'consumption'
PROCESS_TYPE = [PROCESS_TYPE_INGESTION,PROCESS_TYPE_CONSUMPTION]

# Valid values for the log action field
LOG_ACTION_LOG = 'log'
LOG_ACTION = [LOG_ACTION_LOG]

# Valid values for process status field
PROCESS_STATUS_COMPLETE = 'Complete'
PROCESS_STATUS_RUNNING = 'Running'
PROCESS_STATUS_FAILED = 'Failed'
PROCESS_STATUS = [PROCESS_STATUS_COMPLETE,PROCESS_STATUS_RUNNING,PROCESS_STATUS_FAILED]

# Valid values for the load strategy field
LOAD_STRATEGY_INCREMENTAL = 'incremental'
LOAD_STRATEGY_FULL = 'full'
LOAD_STRATEGY_MANUAL = 'manual'
LOAD_STRATEGY_MONTHLY = 'monthly'
LOAD_STRATEGY_CHUNK = 'chunk'
LOAD_STRATEGY_FILE = 'file'
LOAD_STRATEGY = [LOAD_STRATEGY_INCREMENTAL,LOAD_STRATEGY_FULL,LOAD_STRATEGY_MANUAL,
                 LOAD_STRATEGY_MONTHLY,LOAD_STRATEGY_CHUNK,LOAD_STRATEGY_FILE]

# Start Process Keyword arguments
LOAD_STRATEGY_KW = 'load_strategy'
DATE_FORMAT_KW = 'date_format'
START_DATE_KW = 'start_date'
END_DATE_KW = 'end_date'
ACTION_KW = 'action'
STATUS_KW = 'status'
ROW_COUNT_KW = 'row_count'
CURRENCY_DATE_KW = 'currency_date'
FILE_TYPE_KW = 'file_type'


# Row Count Types
BATCH_ROW_CNT_TYPE = 'Batch'
TEMP_TABLE_ROW_CNT_TYPE = 'Temp Table'

# Email content
EMAIL_MESSAGE_TEXT = """{heading}\n
Job Status             :{job_status}
Job Start Time         :{job_start_dttm}
Job PID                :{job_pid}
Job Log File           :{job_log_file}
Job Start Step         :{job_start_step}
Job End Step           :{job_end_step}
Job Last Completed Step:{job_last_completed_step}\n"""
EMAIL_MESSAGE_HTML = """<html><head></head>
<body>
<h1>{heading}</h1>
<p><table>
<tr><td>Job Status</td><td>:</td><td>{job_status}</td></tr>
<tr><td>Job Start Time</td><td>:</td><td>{job_start_dttm}</td></tr>
<tr><td>Job PID</td><td>:</td><td>{job_pid}</td></tr>
<tr><td>Job Log File</td><td>:</td><td>{job_log_file}</td></tr>
<tr><td>Job Start Step</td><td>:</td><td>{job_start_step}</td></tr>
<tr><td>Job End Step</td><td>:</td><td>{job_end_step}</td></tr>
<tr><td>Job Last Completed Step</td><td>:</td><td>{job_last_completed_step}</td></tr>
</table></p>
</body>
</html>"""

# PHG API Constants
CONVERSION_KW = 'conversion'
CLICK_KW = 'click'
CREATIVE_KW = 'creative'
HYPERMEDIA_KW = 'hypermedia'
PAGINATION_KW = 'pagination'
NEXT_PAGE_KW = 'next_page'
LAST_PAGE_KW = 'last_page'
API_KEY_KW = 'api_key'
USER_API_KEY_KW = 'user_api_key_kw'
CAMPAIGN_ID_KW = 'campaign_id'

# Static file support
PKGNAME = "loadmgr.metadata"
STATIC_DIR = 'static'
JOB_FILE_JSON_SCHEMA = 'job_file_json_schema.js'
JOB_FILE_CSV_STEP_SCHEMA = 'job_file_csv_step_schema.js'
HDFS_STAGE_MOVE_TEMPLATE = 'hdfs_file_stage_template.sh'

# Default Date Values
DEFAULT_LOW_DATE = "1900-01-01 00:00:00"
DEFAULT_HIGH_DATE = "2999-12-31 23:59:59"

# Default Config file
DEFAULT_CONFIG_FILE = """
# The type of database
databaseType:
    sqlite

# The name and connection info (location, host, port and/or user) of all the databases
# passwordFile - Works with encrypted password file using the password class
databases: {
    repodb: {
        name: '$PWD/loadmgrdb.db'
        host: ''
        port: 0
        user: ""
        passwordFile: ''
    }
}

# The default field delimiter for the job file
jobFileDelimiter:
    ','

# The character used to surround a variable in the task's args attribute
variableDelimiter:
    '%'

# The maximum number of tasks possible per job execution
queueSizeLimit:
    100

# The location, prefix, extension and date format of the log file names.
logFile: {
    location:
        '$PWD'
    prefix:
        loadmgr
    extension:
        log
    dateFormat:
        '%Y%m%d'
}

# List of email addresses in which to send success or failure emails
SuccessEmailList: []

# List of email addresses in which to send success or failure emails
FailureEmailList: []

# The logging level of detail for success/failure email
# possible values: INFO,WARNING,ERROR,CRITICAL
# INFO = All log messages including warnings and errors.
# WARNING = All warning and error log messages
# ERROR = Only error log messages
# CRITICAL = NOT CURRENTLY SUPPORTED
emailLoggingLevel:
    INFO

# NOT CURRENTLY USED
processSleepTime:
    30

# The number of seconds to wait before checking the status of a paused job to either be stopped or resumed
pauseSleepTime:
    60

# The number of seconds to wait between checking for the existence of a file for a file-based event
waitSleepTime:
    60

# The maximum number of seconds to wait for an event to complete (0 = Infinite)
waitTimeOut:
    360

# Override the default HIVE queue
hiveQueue:
    None

# Hive Settings
#   Format:  "<setting name>=<value>"
hiveSettings: []

# Hive Auxiliary JAR Files
hiveAuxJars: []

# Directory for import config files
ImportConfig:
    ""

# Name Node Host and Port
NameNode: {
    host: ""
    port: 50070
}

# Set the type of client to use for the HDFS package
# Type Valid Options: insecure, kerberos
# Auth Valid Options: REQUIRED, OPTIONAL, DISABLED
# Parameters are passed to HTTPKerberosAuth class, such as principal and service
HDFSClient: {
    type:
        insecure
    auth:
        OPTIONAL
    parameters: {}
}

# Event RESTFUL API
event_api_conn: {
    host:
        ""
    port:
        8080
    endpoint:
        "ds/rest/event"
    token:
        ""
}

# Event API Timeout value
EVENT_API_TIMEOUT:
    5

# Hive Server 2 Configuration for Impyla
hiveserver2: {
    host:
        ""
    port:
        "10000"
    authentication:
        "plain"
    kerberos_service:
        ""
}
"""
