#!/usr/local/bin/python2.7
"""
lmgrwrkr -- Load Manager Worker

lmgrwrkr is an application that executes a job made up of a series of tasks defined in the loader metadata repository.

Version History:
0.01 jwd 09/02/2014
    Add log file name variable replacement for task args
    Add Email for success or failure notification
0.02 jwd 09/11/2014
    Add loadmgr to email subject
    Add processWaitEvents code
    Add SIGTERM capture and clean-up
    Updated to use new version of config now called gencfg.genericConfig 9/16
    Moved the update to start date/time, and job log file  closer to the start 9/17
    Added cfg parameter to repo initialization 9/30
    Removed genericConfig call to use imported config from __init__.py 9/30
0.03 jwd 10/16/2014
    NOTE: Version 0.4 skipped to bring all related modules to same version number
    If a function does not exist the code now fails the job and does not continue even if set to ignore error
    Added warning count for tasks set to ignore errors
    Display warning count for both successful or failed runs
    Added callCreateEvent function to handle create type events associated to a job
    Added check for callCreateEvent function and if not found add to queue
    Added callCreateEvent task to the task list for persistence 10/17
    Added call to reset create events if they exist
0.04 jwd 11/04/2014
    Display warning message when tasks set to ignore error fail
    Update queueTasks to handle step range (start and end step)
    Update replaceArgVariables to use delimiter value set in configuration
0.05 jwd 01/14/2015
    Renamed efxldr from genldr
    Streamline of code using ORM
    Removed Main function - moved code to root level
    Integrated start_process, stop_process and log_data_currency from old load manager into this module
0.06 jwd 03/06/2015
    Updated version to be string instead of float
    Added dictionary creation of the two types of email lists, success and failure, email log setup
    Added error handling for replace_args_variables when no value exists for currency_start_dttm
    Added check for in-memory value for variable currency_start_dttm
        when using format <process_name>.currency_start_dttm
0.07 jwd 05/08/2015
    Fixed a bug when resuming a job that has the <process_name>.<currency_start_dttm> variable (needs to be a string)
    Added some variable initialization to get a green check
0.08 jwd 6/25/2015
    Changed how version is created - master_version + __version__
    Added support for new load strategy chunk in start_process with end_date calculation
    Fixed bug with task args containing a space in the value
    Fixed bug in start_process with manual load strategy and end_date not provided on ingestion - sets to high value
    Added temp_args check in each callable function
    Added log message to indicate when a wait event completed
    Added "encode" method to task_arguments variable to fix issue with shlex function
    Added check in tempargs for start_date and end_date for manual load strategy
    Updated logic for manual load strategy to handle either start or end or both and default if one isn't included
    Changed replace_arg_variables to specifically look for "_" for captured output replacement
    Added split_task_arguments function to remove duplicate code
    Changed date default format for manual load strategy values to YYYY-MM-DD HH24:MI:SS
0.09 jwd 7/15/2015
    HOTFIX: Fixed an application crash when export job is not linked to at least one data currency
0.10 jwd 8/11/2015
    HOTFIX: Converted the output from start_process, stop_process and log_data_currency to a string
            instead of a datetime object (caused an application crash)
0.11 jwd 9/23/2015
    Added load strategy FILE to start_process and stop_process
    Changed start_process to create process_log entry instead of storing in temp values in process table
    Added file_record_count function for counting records in flat files and storing in FileLog table
    Added hdfs_file_stage function for clearing HDFS stage dir and moving local files to HDFS stage dir
    Changed hdfs_file_stage to use hdfs.client.upload method (9/15)
    Added convert_config utility function for converting Kornshell based config files to dictionaries
    Added create_done_file function to pull previously loaded file names for FileLog and write to done file
0.12 jwd 10/20/2015
    Added check for hdfs location for source to exist before creating new sub-directory for file
    Removed adding extension ".bz2" in hdfs_file_stage function
0.13 jwd 10/23/2015
    Changed to use Python2.7 Interpreter
0.14 jwd 10/27/2015
    Removed logging.shutdown() to avoid duplicate emails being sent.
0.15 jwd 10/30/2015
    HOTFIX: Remove newline characters from command output
0.16 jwd 11/11/2015
    HOTFIX: Fixed the newline strip when command output is None
    Changed versioning to 1.3.5 instead of 0.3.5
0.17 jwd 11/18/2015
    Moved command_output.strip() before if statement
    Moved Import statements to top
    Moved program info variables to top
    Added phg_api_call
    Added json_2_csv_preproc
    Add END_DATE_VARIABLE
    Updated function doc strings
    Added validate_date function that uses dateutil.parser to parse date strings without known format
0.18 jwd3 04/25/2016
    Added Debug log statement for output of run_command
    Added a check for a value in end_date before date string formatting in stop_process
0.19 jwd3 05/06/2016 HOTFIX
    Fixed run_command output capture by limiting to only STDOUT instead of both STDOUT and STDERR
    Add log debug message to show output of run_command
    Fixed output of stop_process (currency_end_dttm) failing when currency_end_dttm is null
0.20 jwd3 05/17/2016 HOTFIX
    Updated run_command to write STDERR as error or warning depending on task attribute ignoreError
0.21 jwd3 05/23/2016 HOTFIX
    Fixed run_command exception failure when there are no task arguments.
    Updated run_command to format the STDERR message for log and including the STDOUT on error in the log
0.22 jwd3 05/25/2016 HOTFIX
    Added check for output to stderr in run_command so empty values are not written to log file
0.23 jwd3 10/20/2016 HOTFIX
    Changed jobrun.store() to jobrun.end() in the signal_term_handler function so runs failed with termination
    signal will also have job run end date/time
0.24 jwd3 02/16/02017 Maintenance Release
    Updated path to smtplog and other modules
    Added create_event
0.25 jwd3 02/22/2017 HOTFIX
    Fixed issue with command_output.replace("\n"," ") when command_output is None
0.26 jwd3 04/13/2017 v1.2.4
    Added function to send emails
    Changed program_version to just the version of this module
0.27 jwd3 04/10/2017 PETDEV-28969 v1.3.1
    Replaced dbHive which uses pyhs2 (no longer supported) with the Cloudera Impyla package
    Created a new Hive function callable from a job file to replace the hive_single_job.ksh step, execute_hive_script
    Added Jinja2 template functionality to Hive Scripts and email body for execute_send_email
    Removed convert_config function - Not Used
    Removed PHG web call - never used
    Fixed some weak warnings from inspections
    Updated log data currency since impyla returns datetime objects and not string value of dates
    Added Kerberos support for hdfs python package
0.28 jwd3 05/11/2017 v2.0.0
    Versioning changed to 0.xx where xx is sequential number representing each change to this file
        advanced versioning with major/minor isn't needed
    BugFix - email_body_html was referenced before assignment.  Added initialization.
0.29 jwd3 05/19/2017 v2.0.1
    Failed to add optional parameters to HDFS Kerberos client call
    Renamed hdfs_file_stage to move_file_hdfs - Pure Python option using hdfsCLI package
    Added new hdfs_file_stage that uses bash generated script calling hdfs dfs commands
    Updated execute_hive_script to accept a file or a query
    Added ability to execute hive query via impyla or beeline
    Removed global jobrun variable from queue_tasks
    Changed signature of callable functions to include current_task class object
0.30 jwd3 05/24/2017 v2.1.2
    Added code to write temporary file content to log on failure
    Added task args and temp args to hdfs_file_stage
    Added hdfs_to_local function for moving files to local using dfs -getmerge
    Fixed ability to capture output from run_command function
    Fixed output capture from execute_hive_script (converted from list to csv)
    Fixed task_arguments value when none are present (defaults to empty string instead of None)
    Added default filter for jinja2 variables converted from hiveconf variables for ones not passed in args
0.31 jwd3 06/06/2017 v2.1.3
    Fixed the cli_output_parser - now uses XML Elements format
    Changed output format for Beeline queries from csv2 to xmlelements
    Added some line separators in the log file
    Changed output of temp files to logging level INFO instead of error - prevent false failures on warnings
0.32 jwd3 06/08/2017 v2.1.4
    Fixed bug when querying hive and the query fails it should not try to parse the output
0.33 jwd3 06/15/2017 v2.2.0
    Added check for result set pattern in the CLI output parser
    Added option to suppress success emails
0.34 jwd3 ??/??/?? v2.2.x
    Fixed logging statements (logging changed to logger)
    Fixed potential exception when data currency is an invalid date value
"""
# INSERT imports for modules here.  List system/built-in first and then user-defined
import Queue
import argparse
import json
import logging
# import os
import re
import shlex
import signal
import subprocess
import sys
from calendar import monthrange
from datetime import datetime,timedelta,time
from inspect import isfunction
from time import sleep

from loadmgr.job import EventError
from loadmgr.utils.decorators import LogEntryExit
from loadmgr.metadata.constants import *
from loadmgr.metadata.models import DataCurrencyLog,Link,calculate_duration,FileLog
from loadmgr.metadata.models import Job,JobRun,JobRunFlag,Process,ProcessLog,DataCurrency
from loadmgr.utils import smtplog
from loadmgr import cfg
from loadmgr.utils.event import create_event
from loadmgr.utils.emailer import send_email

__version__ = "0.32"
__date__ = '08-12-2014'
__updated__ = '06/15/2017'

#initialize logging
program_name = os.path.basename(sys.argv[0])
logger = logging.getLogger(program_name.split(".")[0])
logger.setLevel(logging.DEBUG)  # set to lowest level - fine control at handler
formatter = logging.Formatter(LOGGING_FORMAT)

program_version = "v{0}".format(__version__)
program_build_date = str(__updated__)
program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
program_shortdesc = __import__('__main__').__doc__.split("\n")[1]
program_license = '''%s

  Created by user_name on %s.

  Licensed under the Apache License 2.0
  http://www.apache.org/licenses/LICENSE-2.0

  Distributed on an "AS IS" basis without warranties
  or conditions of any kind, either express or implied.

USAGE
''' % (program_shortdesc, str(__date__))


class CLIError(Exception):
    """
    Generic exception to raise and log different fatal errors.
    """
    def __init__(self, msg=''):
        self._msg = msg
        Exception.__init__(self, msg)

    def __repr__(self):
        return self._msg
    __str__ = __repr__


@LogEntryExit(logger)
def process_wait_events():
    """
    If job has wait events set job status to waiting and wait for events to clear
    :return Boolean:
    """
    event_list = jobrun.get_job_event_list(EVENT_ACTION_WAIT)
    if event_list:
        logger.info("Job has {0!s} wait events".format(len(event_list)))
    else:
        logger.info("Job has NO wait events")
        return True
    logger.debug("process each event")
    for event in event_list:
        if not event.complete:
            if cfg.waitTimeOut > 0:
                timeout = ' for {0!s} seconds'.format(cfg.waitTimeOut)
            else:
                timeout = ' indefinitely'
            logger.info("Waiting on {0}{1}".format(event.name,timeout))
            logger.debug("Filename or function name: {0}".format(event.filename or event.function))
            if event.filename:
                logger.debug("Expanded File name= {0}".format(os.path.expandvars(event.filename)))
            try:
                event.wait()
            except EventError as errmsg:
                logger.error("{0}".format(errmsg))
                return False
            if event.timed_out():
                logger.error("Event {0} timed out before completing".format(event.name))
                return False
            else:
                logger.info("Event {0} completed".format(event.name))
        else:
            logger.info("Event {0} already completed".format(event.name))
    logger.info("All event of wait type are complete")
    return True


@LogEntryExit(logger)
def reset_create_events():
    """
    Reset create events if any
    :return Boolean:
    """
    logger.info("Reset any create events")
    try:
        event_list = jobrun.get_job_event_list(EVENT_ACTION_CREATE)
        if event_list:
            logger.debug("Job has create events")
            for event in event_list:
                logger.info("Reset create event [{0}]".format(event.name))
                event.reset()
    except:
        logger.error("Failed to reset create events", exc_info=args.debug)
        return False
    return True


@LogEntryExit(logger)
def queue_tasks(task_list):
    """
    Get the tasks for this job and add them to a queue
    :param task_list: list of tasks to add to the queue
    :return Queue:
    """
    logger.info("Create task queue")
    task_queue_fifo = Queue.Queue(cfg.queueSizeLimit)  # FIFO - First In First Out
    for key in sorted(task_list):
        task_obj = task_list[key]
        if not task_obj.taskCompleted:
            logger.debug("Put step {0!s} on queue".format(key))
            task_queue_fifo.put(task_obj)
    logger.info("Tasks added to queue")
    return task_queue_fifo


@LogEntryExit(logger)
def replace_arg_variables(task_list,task_arguments):
    """
    Replace variables found in args with corresponding task output
    :param task_list:
    :param task_arguments:
    :return:
    """
    variable_delimiter = cfg.variableDelimiter
    new_args = task_arguments
    var_pattern = re.compile('{0}.*?{0}'.format(variable_delimiter))
    logger.debug("Var Pattern= {0}".format(var_pattern.pattern))
    var_list = var_pattern.findall(new_args)
    logger.debug("var_list= {0!r}".format(var_list))
    for var in var_list:
        if var.strip(variable_delimiter) == LOG_FILE_VARIABLE:
            logger.debug("Found log file variable")
            new_args = new_args.replace(var,job_var_dict.get(LOG_FILE_VARIABLE))
        elif var.strip(variable_delimiter) == START_DATE_VARIABLE:
            logger.debug("Found start date variable")
            if job_var_dict.get(START_DATE_VARIABLE):
                start_date = job_var_dict.get(START_DATE_VARIABLE)
                # noinspection PyUnresolvedReferences
                new_args = new_args.replace(var,start_date.strftime("%Y-%m-%d %H:%M:%S"))
            else:
                logger.warning("Currency Start Date variable found without a value stored in memory")
                logger.warning("The 'start_process' step must precede the use of the currency start date variable")
        elif var.count(".") > 0:
            logger.debug("variable contains special format <process_name>.<attribute_name>")
            process_name,attribute_name = var.strip(variable_delimiter).split(".",1)
            if attribute_name == START_DATE_VARIABLE and job_var_dict.get(START_DATE_VARIABLE):
                logger.debug("Pull start date from memory")
                start_date = job_var_dict.get(START_DATE_VARIABLE)
                # noinspection PyUnresolvedReferences
                new_args = new_args.replace(var,start_date.strftime("%Y-%m-%d %H:%M:%S"))
            elif attribute_name == END_DATE_VARIABLE and job_var_dict.get(END_DATE_VARIABLE):
                logger.debug("Pull end date from memory")
                end_date = job_var_dict.get(END_DATE_VARIABLE)
                # noinspection PyUnresolvedReferences
                new_args = new_args.replace(var,end_date.strftime("%Y-%m-%d %H:%M:%S"))
            else:
                try:
                    process = Process.get_process(process_name)
                except Process.DoesNotExist:
                    logger.warning("Process {0} does not exist in repository".format(process_name))
                    logger.warning("Unable to replace variable {0}".format(var))
                    continue
                except:
                    logger.error("Failed to retrieve process from repository", exc_info=args.debug)
                    logger.warning("Unable to replace variable {0}".format(var))
                    continue
                try:
                    process_log = process.get_latest_log()
                except ProcessLog.DoesNotExist:
                    logger.error("Failed to find current process log record for process {0}"
                                 .format(process.process_name))
                    logger.warning("Unable to replace variable {0}".format(var))
                    continue
                try:
                    #replacement_value = process.get_attribute(attribute_name.lower())
                    replacement_value = getattr(process_log,attribute_name.lower())
                    if isinstance(replacement_value,datetime):
                        logger.debug("Convert replacement value date time to a string")
                        replacement_value = replacement_value.strftime("%Y-%m-%d %H:%M:%S")
                    else:
                        logger.debug("Convert replacement value to a string")
                        replacement_value = str(replacement_value)
                    new_args = new_args.replace(var,replacement_value)
                    logger.debug("Var[{0}] replaced with {1}".format(var,replacement_value))
                except AttributeError:
                    logger.warning("Attribute {0} not found for process {1}".format(attribute_name,process_name))
        elif var.count("_") > 0:
            logger.debug("Get captured output from a previous step")
            step = int(var.split('_')[-1].strip(variable_delimiter))
            # add check that step exists or get KeyError
            if task_list.get(step) and task_list[step].taskOutput:
                replacement_value = task_list[step].taskOutput.strip()
                new_args = new_args.replace(var,replacement_value)
                logger.debug("Var[{0}] replaced with {1}".format(var,replacement_value))
            else:
                logger.error('No task output found for variable "{0}"'.format(var))
        else:
            logger.warning("No value found for variable {0}".format(var))
    logger.debug("New args= {0}".format(new_args))
    return new_args


@LogEntryExit(logger)
def signal_term_handler(signum,frame):
    """
    Handle a termination signal sent to the process by cleaning up and saving the job status
    :param signum:
    :param frame:
    :return:
    """
    try:
        logger.debug("Caught signal: {0!s}".format(signum))
        logger.debug("Index of last instruction: {0!s}".format(frame.f_lasti))
        jobrun.set_status(JOB_STATUS_FAILED)
        jobrun.end()
        logger.error("Termination Signal caught")
        # send end event to event api
        create_event(
            {
                "event_category": "error",
                "event_group": "LOAD MANAGER",
                "event_type": EVENT_TYPE,
                "event_subtype": "n/a",
                "event_name": job.job_name,
                "event_action": "end",
                "event_payload": {
                    "signal": str(signum),
                    "last_instruction_index": str(frame.f_lasti)
                }
            }
        )
    except:
        pass
    finally:
        sys.exit(2)


@LogEntryExit(logger)
def create_temp_file(content,job_name,job_step):
    """
    Create a temporary file and save content to it 
    :param content: Content to save in the file
    :param job_name: Nmae of the job - used as a prefix
    :param job_step: Job step number - used as a suffix
    :return: the path and name of the temporary file
    """
    from tempfile import mkdtemp, NamedTemporaryFile, gettempdir
    prefix = 'lmgr-{}-{!s}-'.format(job_name,job_step)
    logger.debug("Prefix: {}".format(prefix))
    logger.info("Root temp directory: {}".format(gettempdir()))
    dir_name = mkdtemp(prefix=prefix)
    logger.info("Temp Directory: {}".format(dir_name))
    with NamedTemporaryFile(dir=dir_name, prefix=prefix,delete=False) as temp_file:
        logging.info("Temp File: {}".format(temp_file.name))
        temp_file.write(content)
    logger.info("Temp File created successfully")
    return temp_file.name


@LogEntryExit(logger)
def delete_temp_file(temp_file_name):
    """
    Remove the temporary file and temporary directory - cleanup
    :param temp_file_name: Name and path of temporary file
    :return: 
    """
    from shutil import rmtree
    logger.info("Remove temporary file {}".format(temp_file_name))
    try:
        rmtree(os.path.dirname(temp_file_name))
    except OSError:
        logger.warn("Failed to remove temporary file!!!", exc_info=args.debug)


@LogEntryExit(logger)
def execute_bash_script(script_content, job_name, job_step, save_results=False):
    """
    Execute a shell command using Bash
    :param script_content: Content of script
    :param job_name: Job Name
    :param job_step: Step within Job
    :param save_results: Boolean flag - save results from command
    :return: 
    """
    script_file = create_temp_file(script_content, job_name, job_step)
    logger.info("Execute Bash Script: {}".format(script_file))
    try:
        proc = subprocess.Popen(['bash',script_file],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT,
                                cwd=os.path.dirname(script_file))
    except OSError as emsg:
        logger.error(emsg, exc_info=args.debug)
        return 1
    logger.info("PID = {0!s}".format(proc.pid))
    logger.info("OUTPUT:")
    output = []
    for line in iter(proc.stdout.readline, ''):
        line = line.decode('utf-8').strip()
        logger.info(line)
        if save_results:
            output.append(line)
    exit_code = proc.wait()
    logger.debug("Exit Code: {}".format(exit_code))
    if args.debug:
        logger.debug("Temporary file {} NOT deleted".format(script_file))
    else:
        # if failure write to log file
        if exit_code:
            logger.info("="*80)
            logger.info("Temporary File Contents")
            logger.info("="*80)
            with open(script_file) as sfile:
                for line in sfile:
                    logger.info(line.strip())
            logger.info("=" * 80)
        delete_temp_file(script_file)
    return exit_code,output


@LogEntryExit(logger)
def build_cli_command(script_name,save_results=False):
    """
    Build the Beeline command
    :param script_name: Name of script file to execute using Beeline
    :param save_results: Boolean flag - save results from query
    :return: The full command line to execute
    """
    logger.info("Build Beeline CLI command")
    hive_bin = 'beeline'
    jdbc_url = "jdbc:hive2://{host}:{port}/default".format(host=cfg.hiveserver2.host,port=cfg.hiveserver2.port)
    logger.debug("JDBC URL: {}".format(jdbc_url))
    if cfg.hiveserver2.authentication == 'kerberos':
        logger.info("Using Kerberos Authentication")
        kerberos_service = cfg.hiveserver2.kerberos_service
        proxy_user = ""
        # FUTURE USE IF NEEDED
        # if cfg.hiveserver2.proxy_user:
        #     proxy_user = ";hive.server2.proxy.user={0}".format(cfg.hiveserver2.proxy_user)
        jdbc_url += ";principal={}{}".format(kerberos_service,proxy_user)
    else:
        logger.info("Authentication: {}".format(cfg.hiveserver2.authentication))
        jdbc_url += ";auth={}".format(cfg.hiveserver2.authentication)

    hive_cmd = [hive_bin,'-u', '"{}"'.format(jdbc_url), '-f', script_name]
    if cfg.hiveserver2.user:
        hive_cmd.extend(['-n',cfg.hiveserver2.user])
    if save_results:
        hive_cmd.append('--showHeader=false --outputformat=xmlelements')
    logger.debug("Hive Command: {}".format(" ".join(hive_cmd)))
    return " ".join(hive_cmd)


@LogEntryExit(logger)
def render_template(metadata, template):
    """
    Generic artifact generator using metadata and template passed
    :param metadata: Dictionary containing the variables and their values used in the template
    :param template: Jinja2 template
    :return: Rendered template
    """
    from jinja2 import Environment, TemplateSyntaxError, UndefinedError, TemplateError
    logger.info("Rendering Template")
    logger.debug("Setup Jinja2 environment")
    env = Environment(trim_blocks=True, lstrip_blocks=True)
    logger.debug("Template= {0}".format(template))
    try:
        artifact_template = env.from_string(template)
    except TemplateSyntaxError as tfs_em:
        logger.error(tfs_em, exc_info=True)
        return "ERROR!ERROR!ERROR - {0}".format(tfs_em.message)
    try:
        rendered = artifact_template.render(metadata)
    except (UndefinedError, TemplateError) as render_em:
        logger.error(render_em, exc_info=True)
        return "ERROR!ERROR!ERROR - {0}".format(render_em.message)
    return rendered


@LogEntryExit(logger)
def cli_output_parser(output):
    """
    Parse CLI output saving the data set
    :param output: List of lines from the CLI output
    :return: List of list of data values
    """
    import xml.etree.ElementTree as ET
    data_xml_pattern = re.compile("(<resultset>.*</resultset>)", re.DOTALL)
    data_search = data_xml_pattern.search(''.join(output))
    if data_search:
        data_tree = ET.fromstring(data_search.group())
        # build list of lists of elements from the XML Tree
        data_list = [leaf for branch in list(data_tree) for leaf in [list(branch)]]
        # Convert each element to the inner text value
        result_set = [[column.text for column in row] for row in data_list]
    else:
        result_set = []
    return result_set


@LogEntryExit(logger)
def query_using_cli(query,task,save_results):
    """
    Execute a query in Hive using the CLI
    :param query: HQL query to execute
    :param task: task object (for job_name and step)
    :param save_results: Boolean flag - return results of query
    :return: 
    """
    logger.info("Execute query using CLI")
    logger.info("Write HQL query to temporary file")
    script_name = create_temp_file(query, task.job_name, task.step)
    logger.info("Temp File: {}".format(script_name))
    logger.info("Build CLI Command")
    command = build_cli_command(script_name,save_results)
    exit_code,output = execute_bash_script(command, task.job_name, task.step, save_results)
    if exit_code == 0 and output and save_results:
        logger.debug("Parse results from CLI output")
        results = cli_output_parser(output)
        logger.debug("Results: {!r}".format(results))
    else:
        results = ''
    if args.debug:
        logger.debug("Temporary file {} NOT deleted".format(script_name))
    else:
        # if failure write to log file
        if exit_code:
            logger.info("=" * 80)
            logger.info("Temporary File Contents")
            logger.info("=" * 80)
            with open(script_name) as sfile:
                for line in sfile:
                    logger.info(line.strip())
            logger.info("=" * 80)
        delete_temp_file(script_name)
    return exit_code,results


@LogEntryExit(logger)
def query_using_impyla(query,save_results):
    """
    Execute a query against a Hive/Impala table
    :param query: HQL query to execute
    :param save_results: Boolean flag - return results of query
    :return: if save_results, return list of list of data records
    """
    logger.info("Query Hive using Impyla")
    logger.debug("Open connection to Hive")
    logger.debug("Import Impyla libraries")
    from impala.dbapi import connect as impyla_connect
    from impala import error
    if cfg.hiveserver2.authentication == 'kerberos':
        logger.debug("Using KERBEROS")
        conn = impyla_connect(host=cfg.hiveserver2.host,
                              port=cfg.hiveserver2.port,
                              auth_mechanism='GSSAPI',
                              kerberos_service_name=cfg.hiveserver2.kerberos_service)
    else:
        logger.debug("Using Auth Mech= {0}".format(cfg.hiveserver2.authentication))
        conn = impyla_connect(host=cfg.hiveserver2.host,
                              port=cfg.hiveserver2.port,
                              auth_mechanism=cfg.hiveserver2.authentication)
    cursor = conn.cursor()
    query_list = [x.strip() for x in query.split(';') if x.strip()]
    logger.debug("Execute Query List: {0!r}".format(query_list))
    try:
        for sql in query_list:
            logger.debug("SQL={0}".format(sql))
            cursor.execute(sql)
            hive_log = cursor.get_log()
            if hive_log:
                logger.info(hive_log)
    except (error.DataError, error.DatabaseError, error.BeeswaxError,
            error.DisconnectedError, error.HiveServer2Error, error.IntegrityError,
            error.InterfaceError, error.InternalError, error.NotSupportedError,
            error.OperationalError, error.ProgrammingError, error.QueryStateError, error.RPCError) as query_error:
        logger.error(str(query_error),exc_info=args.debug)
        cursor.close()
        return 1,None
    logger.debug("Query finished")
    if save_results and cursor.has_result_set:
        logger.debug("Return results")
        # To get results as a dictionary using dictify=False parameter for cursor class
        results = cursor.fetchall()
    else:
        results = None
    cursor.close()
    return 0,results


@LogEntryExit(logger)
def query_hive(query, task, context, save_results=False):
    """
    Execute a query against a Hive table
    :param query: HQL query to execute - can be a single query string or list of query strings
    :param task: The current task 
    :param context: Dictionary of key/value pairs used in template rendering
    :param save_results: Boolean flag indicating whether to return the results of the query
    :return: if save_results, return list of list of data records
    """
    logger.info("prepare query")
    logger.info("Add Queue, Settings and/or Aux JARs to the top of the query")
    script_extras = []
    if cfg.get("hiveQueue"):
        logger.debug("set hive queue={0}".format(cfg.hiveQueue))
        script_extras.extend(["set mapred.fairscheduler.pool={0};\n".format(cfg.hiveQueue)])
    if cfg.get("hiveSettings"):
        logger.debug("set hive settings")
        script_extras.extend(["set {0};\n".format(os.path.expandvars(setting)) for setting in cfg.hiveSettings])
    if cfg.get("hiveAuxJars"):
        logger.debug("Add Auxiliary JARS")
        script_extras.extend(["add jar {0};\n".format(os.path.expandvars(jar)) for jar in cfg.hiveAuxJars])
    if isinstance(query,str):
        query = [query]
    hql_script = "".join(script_extras + query)
    logger.debug("Check for hiveconf and env parameters")
    hive_conf_pattern = re.compile('\${hiveconf:(.*?)\}')
    hive_env_pattern = re.compile('\${env:(.*?)\}')
    expanded_hql_script = hive_env_pattern.sub(lambda mob: os.environ.get(mob.group(1), mob.group(0)), hql_script)
    templated_hql_script = hive_conf_pattern.sub("{{ \g<1>|default('${hiveconf:\g<1>}') }}", expanded_hql_script)
    rendered_hql_script = render_template(context, templated_hql_script)
    if rendered_hql_script.startswith("ERROR!ERROR!ERROR"):
        logger.error("Rendering of templated HQL script failed")
        logger.error(rendered_hql_script)
        return 1,''
    logger.debug("Rendered Template= {0}".format(rendered_hql_script))
    if cfg.hiveserver2.use_beeline or context.get('use_beeline'):
        exit_code,results = query_using_cli(rendered_hql_script,task,save_results)
    else:
        exit_code,results = query_using_impyla(rendered_hql_script,save_results)
    if exit_code:
        logger.error("HQL Failed")
    else:
        logger.info("HQL completed successfully")
    return exit_code, results


@LogEntryExit(logger)
def validate_date(date_string):
    """
    Parse and validate a date string
    :param date_string:
    :return date_value: is datetime object If none, then date string is invalid
    """
    from dateutil.parser import parse
    try:
        date_value = parse(date_string)
    except ValueError as invalid_message:
        date_value = None
        logger.error("Invalid date string {0}: {1!r}".format(date_string,invalid_message),exc_info=args.debug)
    return date_value


@LogEntryExit(logger)
def split_task_arguments(arg_string):
    """
    Split task arguments into a dictionary
    :param arg_string:
    :return:
    """
    logger.debug("try loading a JSON string")
    try:
        arg_dict = json.loads(arg_string)
    except:
        logger.debug("not a JSON string")
    else:
        return arg_dict
    logger.debug("try splitting on equals sign")
    try:
        arg_dict = dict(kv.split("=") for kv in shlex.split(arg_string.encode()))
    except ValueError as err:
        logger.error("Failed to split arguments [{0}]: {1}".format(arg_string,err), exc_info=args.debug)
        arg_dict = {}
    logger.debug("KW arguments= {0!r}".format(arg_dict))
    return arg_dict


####################################################################################################################
#  CALLABLE   FUNCTIONS                                                                                            #
####################################################################################################################
@LogEntryExit(logger)
def start_process(task,task_arguments,temp_args):
    """
    Start a process
    :param task:
    :param task_arguments:
    :param temp_args:
    :return:
    """
    logger.info("Start process {0}".format(task.command))
    logger.debug("Set defaults")
    load_strategy_def = LOAD_STRATEGY_INCREMENTAL
    start_date = None
    end_date = None
    output = None
    # date_format_def = "%Y-%m-%d %H:%M:%S"
    if task_arguments:
        task_arg_dict = split_task_arguments(task_arguments)
        if not task_arg_dict:
            return 1,output
    else:
        task_arg_dict = {}
    load_strategy = temp_args.get(LOAD_STRATEGY_KW) or task_arg_dict.get(LOAD_STRATEGY_KW,load_strategy_def)
    # date_format = temp_args.get(DATE_FORMAT_KW) or task_arg_dict.get(DATE_FORMAT_KW,date_format_def)
    start_date_str = temp_args.get(START_DATE_KW) or task_arg_dict.get(START_DATE_KW)
    end_date_str = temp_args.get(END_DATE_KW) or task_arg_dict.get(END_DATE_KW)
    try:
        process = Process.get_process(task.command)
    except Process.DoesNotExist:
        logger.error("Process {0} does not exist in repository".format(task.command))
        return 1,output
    except:
        logger.error("Failed to determine if process already exists in repository", exc_info=args.debug)
        return 1,output
    logger.debug("Get process configuration")
    currency_date_flag = process.currency_dt_flg
    currency_start_date_offset = process.currency_start_dttm_offset
    currency_end_date_offset = process.currency_end_dttm_offset
    process_type = process.process_type
    logger.debug("Check load strategy value")
    if load_strategy not in LOAD_STRATEGY:
        logger.error("Invalid load strategy: {0}".format(load_strategy))
        return 1,output
    elif load_strategy == LOAD_STRATEGY_MANUAL:
        logger.info("Manual Load Strategy")
        if not start_date_str and not end_date_str:
            logger.error("Must provide at least one date (start or end) for manual load strategy!")
            return 1,output
        if start_date_str:
            start_date = validate_date(start_date_str)
            if not start_date:
                return 1,output
        else:
            start_date = datetime.strptime(DEFAULT_LOW_DATE,"%Y-%m-%d %H:%M:%S")
        if end_date_str:
            end_date = validate_date(end_date_str)
            if not end_date:
                return 1,output
        else:
            end_date = datetime.strptime(DEFAULT_HIGH_DATE,"%Y-%m-%d %H:%M:%S")
    else:  # Load Strategy in Full, Monthly or Incremental, Chunk or File
        logger.info("Load Strategy: {0}".format(load_strategy.upper()))
        # Get the start date value
        if load_strategy == LOAD_STRATEGY_FULL:
            logger.debug("Set start date to default low date for full load strategy")
            start_date = datetime.strptime(DEFAULT_LOW_DATE,"%Y-%m-%d %H:%M:%S")
        elif load_strategy == LOAD_STRATEGY_MONTHLY:
            logger.debug("Set start date to the beginning of the month of the current day - currency start date offset")
            start_date = (datetime.today() - timedelta(days=currency_start_date_offset)).replace(day=1,
                                                                                                 hour=0,
                                                                                                 minute=0,
                                                                                                 second=0)
        elif load_strategy in [LOAD_STRATEGY_INCREMENTAL,LOAD_STRATEGY_CHUNK,LOAD_STRATEGY_FILE]:
            logger.debug("Get the date from the last successful run for the new start date")
            last_run_date = process.get_last_run_date()
            if not last_run_date:
                msg = "No previous runs of process {0} were found - start date will be set to low date value"
                logger.info(msg.format(task.command))
                start_date = datetime.strptime(DEFAULT_LOW_DATE,"%Y-%m-%d %H:%M:%S")
            else:
                #===============================================================
                # logger.info("Add appropriate time to end date/time of last successful run")
                # oneSecond = timedelta(seconds=1)
                # oneDay = timedelta(days=1)
                # if currency_date_flag:
                #     start_date = last_run_date + oneDay
                # else:
                #     start_date = last_run_date + oneSecond
                #===============================================================
                if currency_start_date_offset:
                    logger.debug("Subtract the currency start date offset from the max end date")
                    start_date = last_run_date - timedelta(days=currency_start_date_offset)
                else:
                    start_date = last_run_date
        if process_type == PROCESS_TYPE_INGESTION:
            if load_strategy == LOAD_STRATEGY_CHUNK:
                logger.info("Ingestion process type and Chunk load strategy - calculate end date")
                if currency_end_date_offset == 0:
                    logger.error("Invalid End Date Offset: {0!s}".format(currency_end_date_offset))
                    return 1,output
                elif currency_end_date_offset < 0:
                    logger.debug("Negative offset - calculate from current date")
                    end_date = (datetime.combine(datetime.now().date(),time(0,0,0)) +
                                timedelta(days=currency_end_date_offset))
                else:
                    logger.debug("Positive offset - calculate from start_date")
                    end_date = start_date + timedelta(days=currency_end_date_offset)
            else:
                logger.debug("Ingestion process type and Incremental load strategy - set end date to high date")
                end_date = datetime.strptime(DEFAULT_HIGH_DATE,"%Y-%m-%d %H:%M:%S")
        elif process_type == PROCESS_TYPE_CONSUMPTION:
            if load_strategy == LOAD_STRATEGY_MONTHLY:
                logger.debug("Monthly load strategy - set end date to last day of month of start date at 23:59:59")
                # datetime.combine: combines a date object with a time object (adds 23:59:59 to the date)
                # monthrange (from calendar module) - returns the last day of month given the year and month
                end_date = datetime.combine(start_date.date().replace(day=monthrange(start_date.year,
                                                                                     start_date.month)[1]),
                                            time(23,59,59))
            elif load_strategy == LOAD_STRATEGY_FILE:
                logger.error("Cannot use load strategy FILE with process type CONSUMPTION")
                return 1, output
            elif load_strategy == LOAD_STRATEGY_MANUAL:
                # USE END DATE from command line
                pass
            else:
                table_list = Link.get_table_list(process.process_name)
                date_list = [DataCurrency.get_currency(table_schema,table_name).get_currency_date(currency_date_flag)
                             for table_schema, table_name in table_list]
                if date_list:
                    end_date = min(date_list)
                else:
                    logger.error("Unable to determine the end date")
                    return 1,output
        if currency_date_flag:
            # trunc dates to day
            logger.debug("Currency Date Flag set - Truncate the time")
            start_date = start_date.date()
            end_date = end_date.date() if end_date else None
    logger.debug("Start Date= {0}".format(start_date.strftime("%Y-%m-%d %H:%M:%S")))
    logger.debug("End Date= {0}".format(end_date.strftime("%Y-%m-%d %H:%M:%S") if end_date else ''))
    process_start_dttm = datetime.today()
    process_log = ProcessLog.create(process_key=process,
                                    process_name=process.process_name,
                                    currency_start_dttm=start_date,
                                    currency_end_dttm=end_date,
                                    load_strategy=load_strategy,
                                    process_start_dttm=process_start_dttm,
                                    currency_start_dttm_offset=process.currency_start_dttm_offset,
                                    currency_end_dttm_offset=process.currency_end_dttm_offset,
                                    status=PROCESS_STATUS_RUNNING,
                                    batch_nbr=int("{0!s}{1}".format(process.process_key,
                                                                    process_start_dttm.strftime("%y%m%d%H%M%S"))))
    logger.debug("Update job variable dictionary with currency_start_dttm for use in other steps")
    job_var_dict.update({START_DATE_VARIABLE:process_log.currency_start_dttm,
                         END_DATE_VARIABLE:process_log.currency_end_dttm})
    output = process_log.currency_start_dttm.strftime("%Y-%m-%d %H:%M:%S")
    logger.info("{0} process successfully started".format(task.command))
    return 0,output


@LogEntryExit(logger)
def execute_send_email(task,task_arguments,temp_args):
    """
    Execute the send_email function
    :param task: Required Args - recipient email address list, subject
    :param task_arguments: Optional args - path/file of email body, list of path/file for attachments
    :param temp_args:
    :return:
    """
    logger.info("Execute Send Email {0}".format(task.command))
    logger.debug("Set defaults")
    output = None
    email_body_def = "INTENTIONALLY LEFT BLANK"
    logger.info("Get required arguments defined in JSON format")
    required_args_dict = split_task_arguments(task.command)
    if not required_args_dict:
        return 1, output
    if task_arguments:
        task_arg_dict = split_task_arguments(task_arguments)
        if not task_arg_dict:
            return 1,output
    else:
        task_arg_dict = {}
    recipients = (temp_args.get("recipients")
                  or [x.get("email_address") for x in required_args_dict.get("recipients",list())])
    subject = temp_args.get("subject") or required_args_dict.get("subject")
    if not recipients or not subject:
        logger.error("Missing required argument - recipients or subject")
        logger.debug("Recipients = {0!r}".format(recipients))
        logger.debug("Subject = {0}".format(subject))
        return 1,output
    logger.info("Get email body if exists")
    email_body_arg = os.path.expandvars(task_arg_dict.get("email_body",""))
    logger.debug("Initialize email body variables")
    email_body_html = ""
    email_body_text = ""
    if email_body_arg:
        logger.debug("check if email body is a file")
        if os.path.isfile(email_body_arg):
            logger.debug("email body is a file")
            with open(email_body_arg,'r') as body_file:
                email_body_data = body_file.read()
            if os.path.splitext(email_body_arg)[1] == '.html':
                logger.debug("email body HTML file")
                email_body_html = email_body_data
            else:
                logger.debug("email body TEXT file")
                email_body_text = email_body_data
        else:
            if email_body_arg.startswith('<html>'):
                email_body_html = email_body_arg
            else:
                email_body_text = email_body_arg
    else:
        email_body_text = email_body_def
    params = task_arg_dict.get("params",dict())
    logger.debug("check for params - key/value pairs for variable replacement in template")
    if params:
        logger.debug("render template")
        email_body_rendered = render_template(email_body_text if email_body_text else email_body_html,params)
        if email_body_html:
            email_body_html = email_body_rendered
        else:
            email_body_text = email_body_rendered
    logger.info("check for attachments")
    attachments = [os.path.expandvars(x.get("filename")) for x in task_arg_dict.get("attachments",list())]
    attachment_files = []
    if attachments:
        for filename in attachments:
            if os.path.isfile(filename):
                logger.debug("attachment file found {0}".format(filename))
                attachment_files.append(filename)
            else:
                logger.warn("Attachment [(0}] NOT found".format(filename))
    logger.info('call send_email')
    try:
        send_email(recipients,subject,email_body_text,message_html=email_body_html,files=attachment_files)
    except BaseException as be:
        logger.error("Failed to send email - {0!s}".format(be), exc_info=args.debug)
        return 1,output
    logger.info("Email successfully sent")
    return 0, output


@LogEntryExit(logger)
def stop_process(task,task_arguments,temp_args):
    """
    Stop a process
    :param task:
    :param task_arguments:
    :param temp_args:
    :return:
    """
    logger.info("Stop process {0}".format(task.command))
    logger.debug("Set defaults")
    output = None
    action_def = LOG_ACTION_LOG
    status_def = PROCESS_STATUS_COMPLETE
    row_count_def = 0
    if task_arguments:
        task_arg_dict = split_task_arguments(task_arguments)
        if not task_arg_dict:
            return 1,output
    else:
        task_arg_dict = {}
    action = temp_args.get(ACTION_KW) or task_arg_dict.get(ACTION_KW,action_def)
    if action not in LOG_ACTION:
        logger.error("Invalid action value: {0}".format(action))
        return 1,output
    status = temp_args.get(STATUS_KW) or task_arg_dict.get(STATUS_KW,status_def)
    if status not in PROCESS_STATUS:
        logger.error("Invalid status value: {0}".format(status))
        return 1,output
    row_count = temp_args.get(ROW_COUNT_KW) or task_arg_dict.get(ROW_COUNT_KW,row_count_def)
    try:
        process = Process.get_process(task.command)
    except Process.DoesNotExist:
        logger.error("Process {0} does not exist in repository".format(task.command))
        return 1,output
    except:
        logger.error("Failed to determine if process already exists in repository", exc_info=args.debug)
        return 1,output
    try:
        process_log = process.get_latest_log()
    except ProcessLog.DoesNotExist:
        logger.error("Failed to find current process log record for process {0}".format(process.process_name))
        return 1,output
    currency_date_flag = process.currency_dt_flg
    current_status = process_log.status
    process_type = process.process_type
    current_dttm = datetime.today()  # used for process end dttm
    logger.debug("confirm process was already started before stopping it")
    if current_status == PROCESS_STATUS_RUNNING:
        if process_type == PROCESS_TYPE_INGESTION or (process_type == PROCESS_TYPE_CONSUMPTION
                                                      and process_log.load_strategy == LOAD_STRATEGY_MANUAL):
            table_list = Link.get_table_list(process.process_name)
            if table_list:
                table_schema = table_list[0][0]
                table_name = table_list[0][1]
                logger.debug("Full Table Name: {0}.{1}".format(table_schema,table_name))
                end_date = DataCurrency.get_currency(table_schema,table_name).get_currency_date(currency_date_flag)
                logger.debug("End Date= {0!s}".format(end_date))
                if not end_date:
                    logger.error("Unable to query the end date")
                    return 1,output
                process_log.currency_end_dttm = end_date
            else:
                logger.error("Process {0} is NOT linked to any table with data currency".format(process.process_name))
                return 1,output
            if process_log.load_strategy == LOAD_STRATEGY_FILE:
                logger.debug("Load strategy File")
                # Add any file specific actions here
        elif process_type == PROCESS_TYPE_CONSUMPTION:
            logger.debug("consumption process actions")
            # Add any consumption specific actions here
        process_log.status = status
        process_log.process_end_dttm = current_dttm
        output = process_log.currency_end_dttm.strftime("%Y-%m-%d %H:%M:%S") if process_log.currency_end_dttm else ''
        if not row_count and process.row_cnt_type == BATCH_ROW_CNT_TYPE:
            logger.debug("calculate by counting rows using batch_nbr")
            logger.info("Get row count from counting {0}.batch_nbr".format(process.row_cnt_property))
            sql = "select count(1) as row_count from {0} where batch_nbr = '{1!s}'".format(process.row_cnt_property,
                                                                                           process_log.batch_nbr)
            logger.debug("Execute Query: {0}".format(sql))
            exit_code,results = query_hive(sql,task,task_arg_dict,save_results=True)
            logger.debug("Query returned")
            if exit_code:
                # Query failed - error message already written so just exit
                return 1,output
            logger.debug("get row count query results")
            if results and results[0][0] in (None,'NULL'):
                logger.error("No records found in the table {0} with batch_nbr ({1}) to determine the row count"
                             .format(process.row_cnt_property,process_log.batch_nbr))
                return 1,output
            else:
                row_count = results[0][0]
        logger.debug("row count= {0!s}".format(row_count))
        logger.info("Log process info for {0}".format(task.command))
        process_log.action = action
        process_log.nbr_rows = row_count
        process_log.process_duration = calculate_duration(process_log.process_start_dttm,process_log.process_end_dttm)
        process_log.save()
        logger.debug("Process_log_key= {0!s}".format(process_log.process_log_key))
        logger.info("Process Log saved for {0}".format(task.command))
        logger.info("{0} process successfully stopped".format(task.command))
        return 0,output
    else:
        logger.warning("Process {0} is NOT currently running and cannot be stopped".format(task.command))
        return 0,output


@LogEntryExit(logger)
def move_file_hdfs(task,task_arguments,temp_args):
    """
    Stage file(s) from local stage directory to HDFS stage directory after cleaning HDFS stage directory
    :param task: task object that contains stage directory, source mask and the HDFS stage directory Location
    :param task_arguments: Optional arguments
    :param temp_args: Temporary arguments applicable to current execution only
    :return : exit code and output (for capture)
    """
    logger.info("move_file_hdfs {0}".format(task.command))
    logger.debug("Task Arguments= {0}".format(task_arguments))
    logger.debug("Temp Args= {0}".format(temp_args))
    output = []
    final_exit_code = 0
    logger.debug("import glob and hdfs")
    import glob
    namenode_url = "http://{0}:{1}".format(cfg.NameNode.host,cfg.NameNode.port)
    if cfg.HDFSClient.type.lower() == 'insecure':
        from hdfs import InsecureClient
        client = InsecureClient(namenode_url)
    elif cfg.HdfsClient.type.lower() == 'kerberos':
        from hdfs.ext.kerberos import KerberosClient
        client = KerberosClient(namenode_url,mutual_auth=cfg.HdfsClient.auth,**dict(cfg.HdfsClient.parameters))
    # logger.debug("Set defaults for optional args")
    # file_type_default = None
    # if task_arguments:
    #     task_arg_dict = split_task_arguments(task_arguments)
    #     if not task_arg_dict:
    #         return 1,output
    # else:
    #     task_arg_dict = {}
    # logger.debug('Set optional args values')
    # file_type = temp_args.get(FILE_TYPE_KW) or task_arg_dict.get(FILE_TYPE_KW,file_type_default)
    logger.debug("Parse task.command value into stage dir, source mask and HDFS location")
    required_parameters = map(lambda x: os.path.expandvars(x),task.command.split())
    stage_dir = required_parameters[0]
    file_mask = required_parameters[1]
    hdfs_location = required_parameters[2]
    source_hdfs_location,final_dir_name = os.path.split(hdfs_location)
    logger.info("Clean HDFS Stage Directory")
    logger.debug("Check if HDFS Stage Directory exists")
    if client.status(hdfs_location,strict=False):
        logger.debug("HDFS Stage Directory exists")
        logger.debug("Check if Stage Directory contains any subdirectories")
        if client.content(hdfs_location).get('directoryCount') > 1:
            logger.error("HDFS location {0} has sub-directories and cannot be deleted".format(hdfs_location))
            return 1,None
        logger.info("Delete HDFS Stage Directory")
        success = client.delete(hdfs_location,recursive=True)
        if not success:
            logger.error('Failed to delete the HDFS Stage Dir {0}'.format(hdfs_location))
            return 1,None
    logger.info("Confirm source HDFS location exists")
    if client.status(source_hdfs_location,strict=False):
        logger.info("Create HDFS Stage Directory")
        try:
            client.makedirs(hdfs_location)
        except:
            logger.error("Failed to create HDFS stage directory {0}".format(hdfs_location), exc_info=args.debug)
            return 1,None
    else:
        logger.error("Source HDFS location missing [{0}]".format(source_hdfs_location))
        return 1,None
    logger.info("Get stage file list")
    stage_file_list = glob.glob(os.path.join(os.path.expandvars(stage_dir),file_mask))
    logger.info("Copy file(s) to HDFS")
    for file_name in stage_file_list:
        logger.debug("move file {0}".format(file_name))
        try:
            client.upload(hdfs_location,file_name)
        except:
            logger.error("Failed to move the file {0} to {1}".format(file_name,hdfs_location), exc_info=args.debug)
            final_exit_code = 1
    output = ''.join(output)
    return final_exit_code,output


@LogEntryExit(logger)
def hdfs_file_stage(task,task_arguments,temp_args):
    """
    Stage file(s) from local stage directory to HDFS stage directory after cleaning HDFS stage directory
    :param task: Task object that contains stage directory, source mask and the HDFS stage directory Location
    :param task_arguments: Optional arguments
    :param temp_args: Temporary arguments applicable to current execution only
    :return : exit code and output (for capture)
    """
    import pkg_resources
    output = ''
    logger.info("hdfs_file_stage {0}".format(task.command))
    logger.debug("Task Arguments= {0}".format(task_arguments))
    logger.debug("Temp Args= {0}".format(temp_args))
    if task_arguments:
        task_arg_dict = split_task_arguments(task_arguments)
        if not task_arg_dict:
            return 1,output
    else:
        task_arg_dict = {}
    if temp_args:
        logger.debug("Update task args with temp args")
        task_arg_dict.update(temp_args)
    logger.debug("Parse task.command value into stage dir, source mask and HDFS location and store in context")
    required_parameters = map(lambda x: os.path.expandvars(x),task.command.split())
    context = {'stage_dir': required_parameters[0],
               'file_mask': required_parameters[1],
               'hdfs_location': required_parameters[2]}
    context.update(task_arg_dict)
    template = pkg_resources.resource_string(PKGNAME,os.path.join(STATIC_DIR,HDFS_STAGE_MOVE_TEMPLATE))
    rendered_template = render_template(context,template)
    logger.debug("Rendered Template:")
    logger.debug(rendered_template)
    final_exit_code,output = execute_bash_script(rendered_template,task.job_name,task.step)
    if final_exit_code:
        logger.error("Failed to move the file {0[stage_dir]}/{0[file_mask]} to {0[hdfs_location]}".format(context))
    return final_exit_code,output


# 05/25/2017 - Decided not implement but left the code for future use if needed
# @LogEntryExit(logger)
# def hdfs_to_local(task,task_arguments,temp_args):
#     """
#     Combine HDFS files into a single file and copy to a local FS path
#     :param task: Task object that contains stage directory, source mask and the HDFS stage directory Location
#     :param task_arguments: Optional arguments
#     :param temp_args: Temporary arguments applicable to current execution only
#     :return : exit code and output (for capture)
#     """
#     logger.info("hdfs_to_local {0}".format(task.command))
#     logger.debug("Task Arguments= {0}".format(task_arguments))
#     logger.debug("Temp Args= {0}".format(temp_args))
#     output = ''
#     if task_arguments:
#         task_arg_dict = split_task_arguments(task_arguments)
#         if not task_arg_dict:
#             return 1,output
#     else:
#         task_arg_dict = {}
#     if temp_args:
#         logger.debug("Update task args with temp args")
#         task_arg_dict.update(temp_args)
#     logger.debug("Parse task.command value into HDFS path, local path/filename and store in context")
#     required_parameters = map(lambda x: os.path.expandvars(x),task.command.split())
#     context = {'hdfs_location': required_parameters[0],
#                'local_path': required_parameters[1]}
#     context.update(task_arg_dict)
#     template = "hdfs dfs -getmerge {hdfs_location} {local_path}".format(**context)
#     rendered_template = render_template(context,template)
#     logger.debug("Rendered Template:")
#     logger.debug(rendered_template)
#     final_exit_code,output = execute_bash_script(rendered_template,task.job_name,task.step)
#     if final_exit_code:
#         logger.error("Failed to copy the file {hdfs_location} to {local_path}".format(**context))
#     return final_exit_code,output


@LogEntryExit(logger)
def file_record_count(task,task_arguments,temp_args):
    """
    Count records in staged files
    :param task: Task object that contains the process name, the stage directory and the source mask
    :param task_arguments: (Optional arguments) date_format= file_type=
    :param temp_args: Temporary arguments applicable to current execution only
    :return : exit code and output (for capture)
    """
    logger.info("file_record_count {0}".format(task.command))
    logger.debug("import glob and bz2")
    import glob
    import bz2
    logger.debug("Set defaults")
    output = None
    file_type_default = None
    date_format_default = None
    if task_arguments:
        task_arg_dict = split_task_arguments(task_arguments)
        if not task_arg_dict:
            return 1,output
    else:
        task_arg_dict = {}
    logger.debug('Set optional args values')
    file_type = temp_args.get(FILE_TYPE_KW) or task_arg_dict.get(FILE_TYPE_KW,file_type_default)
    date_format = temp_args.get(DATE_FORMAT_KW) or task_arg_dict.get(DATE_FORMAT_KW,date_format_default)
    logger.debug('file_type = {0} -- date_format = {1}'.format(file_type,date_format))
    logger.debug("Parse task.command value into process name, the stage dir and the file mask")
    required_parameters = map(lambda x: os.path.expandvars(x),task.command.split())
    process_name = required_parameters[0]
    stage_dir = required_parameters[1]
    file_mask = required_parameters[2] + ".bz2"
    logger.debug("Get Process and Process Log records")
    try:
        process = Process.get_process(process_name)
    except Process.DoesNotExist:
        logger.error("Process {0} does not exist in repository".format(process_name))
        return 1,output
    except:
        logger.error("Failed to determine if process already exists in repository", exc_info=args.debug)
        return 1,output
    try:
        process_log = process.get_latest_log()
    except ProcessLog.DoesNotExist:
        logger.error("Failed to find current process log record for process {0}".format(process.process_name))
        return 1,output
    if date_format:
        logger.debug("Set regular expression pattern for date format")
        format_translation = {'MM':('[0-1][0-9]','%m'),'dd':('[0-3][0-9]','%d'),'yyyy':('201[5-9]','%Y')}
        regexp_pattern = date_format[:]
        for format_code,value in format_translation.items():
            regexp_pattern = regexp_pattern.replace(format_code,value[0])
            date_format = date_format.replace(format_code,value[1])
        date_pattern = re.compile('(' + regexp_pattern + ')')
        logger.debug("Date Format = {0} -- date_pattern = {1}".format(date_format,date_pattern.pattern))
    else:
        date_pattern = None
        date_format = None
    logger.info("Get stage file list")
    stage_file_list = glob.glob(os.path.join(os.path.expandvars(stage_dir),file_mask))
    stage_file_list.sort()
    total_record_count = 0
    for file_name in stage_file_list:
        logger.debug("count records")
        cf = bz2.BZ2File(file_name,'r')
        num_lines = sum(1 for _ in cf)
        cf.close()
        if date_pattern:
            match = date_pattern.search(file_name)
            if match:
                file_date = datetime.strptime(match.group(),date_format)
            else:
                file_date = None
        else:
            file_date = None
        total_record_count += num_lines
        logger.debug("Insert record into File Log table")
        FileLog.create(process_key=process,
                       process_log_key=process_log,
                       file_name=os.path.basename(file_name),
                       record_count=num_lines,
                       file_type=file_type,
                       file_date=file_date)
    output = str(total_record_count)
    return 0,output


@LogEntryExit(logger)
def create_done_file(task,task_arguments,temp_args):
    """
    Created done file, a file containing a list of previously processed file names
    :param task: Task object that dontains the process name and the name of done file (including path)
    :param task_arguments: Optional arguments
    :param temp_args: Temporary arguments applicable to current execution only
    :return : exit code and output (for capture)
    """
    logger.info("create_done_file {0}".format(task.command))
    logger.debug("Set defaults")
    output = None
    # <some_arg>_default = None
    if task_arguments:
        task_arg_dict = split_task_arguments(task_arguments)
        if not task_arg_dict:
            return 1,output
    else:
        task_arg_dict = {}
    logger.debug('Task Args: {0!s}'.format(task_arg_dict))
    logger.debug('Temp Args: {0!s}'.format(temp_args))
    # <some_arg> = temp_args.get(<some_arg>_KW) or task_arg_dict.get(<some_arg>,<some_arg>_default)
    logger.debug("Parse task.command value into process name and import config filename")
    required_parameters = map(lambda cmd: os.path.expandvars(cmd),task.command.split())
    process_name = required_parameters[0]
    done_file = required_parameters[1]
    logger.debug("Get Process and Process Log records")
    try:
        process = Process.get_process(process_name)
    except Process.DoesNotExist:
        logger.error("Process {0} does not exist in repository".format(process_name))
        return 1,output
    except:
        logger.error("Failed to determine if process already exists in repository", exc_info=args.debug)
        return 1,output
    try:
        # get list of file names
        filename_list = [(x.file_name,
                          str(x.record_count),
                          x.file_date.strftime('%Y-%m-%d') if x.file_date else x.create_dttm.strftime('%Y-%m-%d'))
                         for x in process.file]
    except FileLog.DoesNotExist:
        logger.warning("No files previously loaded for process {0}".format(process.process_name))
        filename_list = []
    except:
        logger.error("Failed to get previously loaded file names for process {0}".format(process.process_name),
                     exc_info=args.debug)
        return 1,output
    logger.info("Check if done file path exists")
    if os.path.isdir(os.path.split(done_file)[0]):
        logger.debug("done file path exists")
        logger.info("Write done file")
        try:
            with open(done_file,'w') as outfile:
                for fn in filename_list:
                    outfile.write(','.join(fn) + "\n")
        except:
            logger.error("Failed to create done file [{0}]".format(done_file), exc_info=args.debug)
            return 1,output
    else:
        logger.error("Invalid path [{0}]".format(done_file), exc_info=args.debug)
        return 1,output
    output = done_file
    return 0,output


@LogEntryExit(logger)
def execute_hive_script(task,task_arguments,temp_args):
    """
    Execute a Hive HQL script
    :param task:  Task object that contains required arguments - Path and name of file containing Hive HQL or a Query
    :param task_arguments:  Optional Arguments - Configuration parameters
    :param temp_args:  runtime arguments
    :return:
    """
    logger.info("Execute Hive Script")
    hql_query = os.path.expandvars(task.command.strip())
    output = None
    if task_arguments:
        task_arg_dict = split_task_arguments(task_arguments)
        if not task_arg_dict:
            return 1,output
    else:
        task_arg_dict = {}
    if temp_args:
        logger.info("Update task args with temp args")
        task_arg_dict.update(temp_args)
    logger.debug("Check if command value is a file or a query")
    if os.path.isfile(hql_query):
        logger.info("Load Hive HQL script from file into memory {}".format(hql_query))
        with open(hql_query,'r') as hql_file:
            # Process each line looking for commented lines to remove
            hql_lines = [line for line in hql_file if line.strip() and not line.lstrip().startswith('--')]
    else:  # command value is a query
        logger.info("HQL Query found")
        hql_lines = [hql_query]
    exit_code,output = query_hive(hql_lines,task,task_arg_dict,save_results=task.captureOutput)
    if output:
        # convert list of lists to a single value
        output = '\n'.join([','.join(row) for row in output])
    return exit_code, output


@LogEntryExit(logger)
def log_data_currency(task,task_arguments,temp_args):
    """
    Log data currency info
    :param task:  Task Object
    :param task_arguments: Task arguments (job_step_arguments with replaced variables)
    :param temp_args: Job run specific argument overrides
    :return:
    """
    table_schema = task.command.split()[0]
    table_name = task.command.split()[1]
    logger.info("Log data currency info for {0}.{1}".format(table_schema,table_name))
    logger.debug("Set defaults")
    output = None
    # date_format_def = "%Y%m%d%H%M%S"
    if task_arguments:
        task_arg_dict = split_task_arguments(task_arguments)
        if not task_arg_dict:
            return 1,output
    else:
        task_arg_dict = {}
    currency_date = temp_args.get(CURRENCY_DATE_KW) or task_arg_dict.get(CURRENCY_DATE_KW)
    # date_format = temp_args.get(DATE_FORMAT_KW) or task_arg_dict.get(DATE_FORMAT_KW,date_format_def)
    logger.debug("get data currency info")
    try:
        currency = DataCurrency.get_currency(table_schema, table_name)
    except DataCurrency.DoesNotExist:
        logger.info("Table {0}.{1} not currently configured for data currency")
        return 1,output
    except:
        logger.error("Failed to retrieve data currency configuration for table {0}.{1}"
                     .format(table_schema,table_name), exc_info=args.debug)
        return 1,output
    currency_date_offset_flag = currency.currency_dt_offset_flg
    currency_column = currency.currency_dttm_column
    currency_query = currency.currency_dttm_query
    existing_currency_date = currency.get_currency_date(currency_date_flag=False)
    logger.debug("Set date difference to one day if currency date offset flag is true; otherwise set to zero")
    date_diff = timedelta(days=currency_date_offset_flag)
    if currency_date:
        currency_date = validate_date(currency_date)
        if not currency_date:
            return 1,output
        logger.debug("Currency Date= {0!s}".format(currency_date))
    else:
        logger.info("Get data currency date from {0}.{1}.{2}".format(table_schema,table_name,currency_column))
        if currency_query:
            logger.debug("Use override currency query")
            sql = currency_query
        else:
            sql = "select max({2}) as data_currency_date from {0}.{1}".format(table_schema,table_name,currency_column)
        logger.debug("Execute Query: {0}".format(sql))
        exit_code,results = query_hive(sql,task,task_arg_dict,save_results=True)
        logger.debug("Query returned")
        if exit_code:
            return 1,output
        if results and results[0][0] in (None,'NULL'):
            logger.error("No records found in the table {0}.{1} to determine the data currency date"
                         .format(table_schema,table_name))
            return 1,output
        currency_date = validate_date(results[0][0])
        if not currency_date:
            logger.error("Invalid date value: {}".format(results[0][0]))
            return 1,output
    if not existing_currency_date or existing_currency_date != currency_date:
        logger.debug("Set currency date field to currency_date excluding the time and subtract the date diff")
        try:
            data_currency_log = DataCurrencyLog.log_data_currency(currency,
                                                                  currency_date,
                                                                  (currency_date.date() - date_diff))
        except:
            logger.error("Failed to log data currency", exc_info=args.debug)
            return 1,output
        logger.debug("Data_currency_log_key= {0!s}".format(data_currency_log.data_currency_log_key))
        logger.info("Data currency value logged for table {0}.{1}".format(table_schema,table_name))
    else:
        logger.info("Data currency value same as previous run and not inserted for table {0}.{1}".format(table_schema,
                                                                                                         table_name))
    try:
        output = currency_date.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        logger.warn("Invalid Date Value [{0!r}]".format(currency_date))
        output = "1900-01-01 00:00:00"
    return 0,output


@LogEntryExit(logger)
def call_create_event(task,task_arguments,temp_args):
    """
    Call the create method for each event
    :param task:
    :param task_arguments:
    :param temp_args:
    :return:
    """
    logger.debug("Command= {0}".format(task.command))
    logger.debug("Task Arguments= {0}".format(task_arguments))
    logger.debug("Temp Args= {0!r}".format(temp_args))
    try:
        event_list = [x for x in jobrun.job_event if x.actionType == EVENT_ACTION_CREATE]
        if event_list:
            logger.info("Job has {0!s} create events".format(len(event_list)))
        else:
            logger.error("Create type events expected for job, but NOT found")
            return 1,"See Log File"
    except:
        logger.error("Failed to get create event list", exc_info=args.debug)
        return 1,"See Log File"
    logger.debug("process each event")
    for event in event_list:
        if not event.complete:
            logger.info("Executing create for {0}".format(event.name))
            logger.debug("Filename or function name: {0}".format(event.filename or event.function))
            if event.filename:
                logger.debug("Expanded File name= {0}".format(os.path.expandvars(event.filename)))
            try:
                event.create()
            except EventError as emsg:
                logger.error("{0}".format(emsg))
                return 1,"See Log File"
        else:
            logger.info("Event {0} already completed".format(event.name))
    logger.info("All event of create type are complete")
    return 0,None


@LogEntryExit(logger)
def run_command(task,task_arguments,temp_args):
    """
    Run OS command
    :param task:
    :param task_arguments:
    :param temp_args:
    :return:
    """
    logger.info("starting the command {0}".format(task.command))
    logger.debug("Task Args: {}".format(task_arguments))
    logger.debug("Temp Args= {0!r}".format(temp_args))
    if task_arguments:
        command = "{} {}\n".format(task.command, task_arguments)
    else:
        command = "{}\n".format(task.command)
    logger.debug("command: {0!s}".format(command.strip()))
    exit_code,output = execute_bash_script(os.path.expandvars(command), task.job_name, task.step,
                                           save_results=task.captureOutput)
    return exit_code,''.join(output)

"""
########################################################################################################################
########################################################################################################################
########################################################################################################################
                                                                                                                         
   MMMMMMMM               MMMMMMMM                    AAA                    IIIIIIIIII     NNNNNNNN        NNNNNNNN     
   M:::::::M             M:::::::M                   A:::A                   I::::::::I     N:::::::N       N::::::N     
   M::::::::M           M::::::::M                  A:::::A                  I::::::::I     N::::::::N      N::::::N     
   M:::::::::M         M:::::::::M                 A:::::::A                 II::::::II     N:::::::::N     N::::::N     
   M::::::::::M       M::::::::::M                A:::::::::A                  I::::I       N::::::::::N    N::::::N     
   M:::::::::::M     M:::::::::::M               A:::::A:::::A                 I::::I       N:::::::::::N   N::::::N     
   M:::::::M::::M   M::::M:::::::M              A:::::A A:::::A                I::::I       N:::::::N::::N  N::::::N     
   M::::::M M::::M M::::M M::::::M             A:::::A   A:::::A               I::::I       N::::::N N::::N N::::::N     
   M::::::M  M::::M::::M  M::::::M            A:::::A     A:::::A              I::::I       N::::::N  N::::N:::::::N     
   M::::::M   M:::::::M   M::::::M           A:::::AAAAAAAAA:::::A             I::::I       N::::::N   N:::::::::::N     
   M::::::M    M:::::M    M::::::M          A:::::::::::::::::::::A            I::::I       N::::::N    N::::::::::N     
   M::::::M     MMMMM     M::::::M         A:::::AAAAAAAAAAAAA:::::A           I::::I       N::::::N     N:::::::::N     
   M::::::M               M::::::M        A:::::A             A:::::A        II::::::II     N::::::N      N::::::::N     
   M::::::M               M::::::M       A:::::A               A:::::A       I::::::::I     N::::::N       N:::::::N     
   M::::::M               M::::::M      A:::::A                 A:::::A      I::::::::I     N::::::N        N::::::N     
   MMMMMMMM               MMMMMMMM     AAAAAAA                   AAAAAAA     IIIIIIIIII     NNNNNNNN         NNNNNNN     

########################################################################################################################
########################################################################################################################
########################################################################################################################
"""

# initialize
jobrun = None
args = None

try:
    # Setup argument parser
    parser = argparse.ArgumentParser(description=program_license, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-D", "--debug", action="store_true", help="Debug level logging [default: %(default)s]")
    parser.add_argument("-L", "--log-file", dest="log_file", help="Name and path of log file [default: %(default)s]")
    parser.add_argument('-V', '--version', action='version', version=program_version_message)
    parser.add_argument("--no-success-email", action="store_true", help="Suppress success emails")
    parser.add_argument("job_name", action='store', help="Name of job to execute")
    
    # Process arguments
    args = parser.parse_args()
    
    base_log_file = "{0}_{1}_{2}_{3}.{4}".format(os.path.join(cfg.logFile.location,cfg.logFile.prefix),
                                                 program_name.split(".")[0],
                                                 args.job_name,
                                                 datetime.now().strftime("%Y%m%d%H%M%S"),
                                                 cfg.logFile.extension)
    job_log_file = os.path.expandvars(args.log_file or base_log_file)
    if job_log_file:
        fh = logging.FileHandler(job_log_file)
        if args.debug:
            fh.setLevel(logging.DEBUG)
        else:
            fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    else:
        raise CLIError("CRITICAL ERROR: Log file name and path NOT defined in configuration or as CL parameter\n")

    logger.debug("Define job variables dictionary and seed with job log file")
    job_var_dict = {LOG_FILE_VARIABLE:job_log_file}

    # setup email handler
    email_list = {'success':cfg.SuccessEmailList,'failure':cfg.FailureEmailList}
    logger.debug("email list= {0!r}".format(email_list))
    eh = smtplog.BufferingSMTPHandler(email_list,
                                      "{{status}}-loadmgr:{name}".format(name=args.job_name),
                                      on_failure_only=True)
    if args.debug:
        eh.setLevel(logging.DEBUG)
    else:
        eh.setLevel(logging.getLevelName(cfg.emailLoggingLevel))
    eh.setFormatter(formatter)
    logger.addHandler(eh)
    
    #=======================================================================
    # # if no screen output is desired, comment out this section
    # ch = logging.StreamHandler()
    # ch.setFormatter(formatter)
    # if args.silent:
    #     ch.setLevel(logging.CRITICAL)
    # else:
    #     if args.debug:
    #         ch.setLevel(logging.DEBUG)
    #     else:
    #         ch.setLevel(logging.INFO)
    # logger.addHandler(ch)        
    #=======================================================================

    # Start the program
    logger.info("Start {0} ".format(program_name))
    
    #===========================================================================
    # logger.debug("Environment as follows:")
    # for k,v in os.environ.items():
    #     logger.debug("{0} = {1}".format(k,v))
    #===========================================================================
    
    logger.debug("Check if job exists in repository and if so retrieve the data")
    try:
        job = Job.get_job(args.job_name)
    except Job.DoesNotExist:
        if args.debug:
            logger.exception("Invalid job name")
        raise CLIError("Job {0} does not exist".format(args.job_name))
    if job:
        logger.debug("Job Found {0}".format(job.job_name))
        try:
            jobrun = JobRun.get_job_run(job)
            logger.debug("Job run found; now set start time; Status= {0}".format(jobrun.job_status))
            if jobrun.job_status != JOB_STATUS_READY:
                logger.debug("Traceback", exc_info=True)
                raise CLIError("Job run not ready: status={0}".format(jobrun.job_status))
            jobrun.start(job_log_file)
        except:
            logger.debug("Traceback", exc_info=True)
            raise CLIError("Failed to get job run data or start job run")
    else:
        logger.debug("Traceback", exc_info=True)
        raise CLIError("Job {0} does NOT exist".format(args.job_name))

    signal.signal(signal.SIGTERM, signal_term_handler)  # set trap for termination signal
    queue_count = 0
    # send start event to event api
    if job.job_name.count('import') > 0:
        EVENT_TYPE = 'import'
    elif job.job_name.count('export') > 0:
        EVENT_TYPE = 'export'
    else:
        EVENT_TYPE = 'process'
    create_event(
        {
            "event_category": "info",
            "event_group": "LOAD MANAGER",
            "event_type": EVENT_TYPE,
            "event_subtype": "n/a",
            "event_name": job.job_name,
            "event_action": "start",
            "event_payload": None
        }
    )
    if not reset_create_events():  # Reset any create events
        failure_count = 1
        no_errors = False
    else:
        task_queue = queue_tasks(jobrun.job_queue)
        queue_count = task_queue.qsize()
        logger.debug("Queue Size= {0!s}".format(queue_count))
        if queue_count == 0:
            failure_count = 1
            no_errors = False
        else:
            logger.info("Check for wait type events")
            if process_wait_events():
                failure_count = 0
                no_errors = True
                jobrun.set_status(JOB_STATUS_RUNNING)
            else:
                failure_count = 1
                no_errors = False  # failed to process all wait events
                logger.error("All wait events were not satisfied within the time limit")
    task_completed_count = 0
    rc = 0
    warning_count = 0
    # Do NOT change to use variable queue_count - need to dynamically check size each loop until Q is empty
    while no_errors and task_queue.qsize() > 0:
        logger.debug("Check job run status")
        flag = JobRunFlag.get_flag(jobrun)
        #currentStatus = JobRun.getStatus(jobrun.job_run_id)
        logger.debug("Pause Flag= {0!s}".format(flag.pause_flag))
        logger.debug("Stop Flag= {0!s}".format(flag.stop_flag))
        if flag.pause_flag:
            logger.info("Job paused - sleep {0!s} second(s)".format(cfg.pauseSleepTime))
            if jobrun.job_status != JOB_STATUS_PAUSED:
                jobrun.set_status(JOB_STATUS_PAUSED)
            sleep(cfg.pauseSleepTime)
            continue
        elif flag.stop_flag:
            logger.info("Job stopped")
            jobrun.set_status(JOB_STATUS_STOPPED)
            failure_count = -1  # to prevent updating the status to completed or failed
            break
        elif jobrun.job_status == JOB_STATUS_PAUSED and not flag.pause_flag:
            logger.info("Job Un-Paused")
            jobrun.set_status(JOB_STATUS_RUNNING)
        #=======================================================================
        # else:
        #     logger.error("Invalid flag value for job")
        #     failureCount = 1
        #     break
        #=======================================================================
        current_task = task_queue.get()
        current_task.job_name = job.job_name
        function_name = current_task.taskFunction
        if function_name in globals() and isfunction(globals()[function_name]):
            logger.debug("function {0} found in globals".format(function_name))
            func = globals()[function_name]
            logger.debug("Check for arg variables and replace with defined value")
            if current_task.args:  # if there are no args, then no need to replace arg variables
                task_args = replace_arg_variables(jobrun.job_queue,current_task.args)
            else:
                task_args = ''
            # send step start event to event api
            create_event(
                {
                    "event_category": "info",
                    "event_group": "LOAD MANAGER",
                    "event_type": EVENT_TYPE,
                    "event_subtype": "step_start",
                    "event_name": job.job_name,
                    "event_action": "audit",
                    "event_payload": {
                        "step": current_task.step,
                        "description": current_task.description,
                        "task_function": current_task.taskFunction,
                        "command": current_task.command,
                        "args": current_task.args,
                        "ignore_error": current_task.ignoreError,
                        "capture_output": current_task.captureOutput
                    }
                }
            )
            logger.info("Task {0!s}:[{1}] started".format(current_task.step,current_task.description))
            start_time = datetime.today()
            logger.debug("Start Time for step {0}: {1}".format(current_task.step,start_time))
            return_code,command_output = func(current_task,task_args,jobrun.job_temp_args)
            logger.debug("Return Code= {0!s}".format(return_code))
            logger.debug("Ignore Error= {0}".format(current_task.ignoreError))
            end_time = datetime.today()
            logger.debug("End Time for step {0}: {1}".format(current_task.step,end_time))
            if command_output:
                command_output = command_output.strip().replace("\n"," ")
            if return_code > 0 and not current_task.ignoreError:
                current_task.store_results(start_time,
                                           end_time,
                                           TASK_STATUS_FAILURE,
                                           command_output)
                logger.error("Task {0!s}:[{1}] FAILED".format(current_task.step,current_task.description))
                no_errors = False
                failure_count += 1
                # send step end event to event api
                create_event(
                    {
                        "event_category": "error",
                        "event_group": "LOAD MANAGER",
                        "event_type": EVENT_TYPE,
                        "event_subtype": "step_end",
                        "event_name": job.job_name,
                        "event_action": "audit",
                        "event_payload": {
                            "start_time": start_time,
                            "end_time": end_time,
                            "command_output": command_output
                        }
                    }
                )
            else:
                if current_task.ignoreError and return_code > 0:
                    warning_count += 1
                    task_status = TASK_STATUS_WARNING
                    event_cat = 'warning'
                else:
                    task_status = TASK_STATUS_SUCCESS
                    task_completed_count += 1
                    event_cat = 'info'
                current_task.store_results(start_time,end_time,task_status,command_output)
                current_task.set_to_completed()
                logger.info("Task {0!s}:[{1}] completed {2}"
                            .format(current_task.step,
                                    current_task.description,
                                    'successfully' if task_status == TASK_STATUS_SUCCESS else 'with a warning'))
                jobrun.save_queue(current_task.step)
                # send step end event to event api
                create_event(
                    {
                        "event_category": event_cat,
                        "event_group": "LOAD MANAGER",
                        "event_type": EVENT_TYPE,
                        "event_subtype": "step_end",
                        "event_name": job.job_name,
                        "event_action": "audit",
                        "event_payload": {
                            "start_time": start_time,
                            "end_time": end_time,
                            "command_output": command_output
                        }
                    }
                )
        else:
            logger.error("Invalid function or non-existent function [{0}]".format(function_name))
            failure_count += 1
            no_errors = False
    # end of the while loop
    if failure_count == 0:
        logger.info("{0!s} task(s) completed successfully".format(task_completed_count))
        if warning_count:
            logger.warning("{0!s} task(s) completed with a warning".format(warning_count))
        if queue_count == (task_completed_count + warning_count):
            jobrun.set_status(JOB_STATUS_COMPLETED)
            logger.info("Job {0} completed".format(args.job_name))
        else:
            jobrun.set_status(JOB_STATUS_STOPPED)
            logger.error("Encountered a situation where no tasks failed, but not all were completed.")
        rc = 0
        if args.no_success_email:
            logger.info("No success email sent")
        else:
            email_message_text = EMAIL_MESSAGE_TEXT.format(heading="{0} completed successfully".format(job.job_name),
                                                           job_status="SUCCESS",
                                                           job_start_dttm=jobrun.job_start_dttm,
                                                           job_pid=jobrun.job_pid,
                                                           job_log_file=jobrun.job_log_file,
                                                           job_start_step=jobrun.job_start_step,
                                                           job_end_step=jobrun.job_end_step,
                                                           job_last_completed_step=jobrun.job_last_completed_step)
            email_message_html = EMAIL_MESSAGE_HTML.format(heading="{0} completed successfully".format(job.job_name),
                                                           job_status="SUCCESS",
                                                           job_start_dttm=jobrun.job_start_dttm,
                                                           job_pid=jobrun.job_pid,
                                                           job_log_file=jobrun.job_log_file,
                                                           job_start_step=jobrun.job_start_step,
                                                           job_end_step=jobrun.job_end_step,
                                                           job_last_completed_step=jobrun.job_last_completed_step)
            try:
                send_email(cfg.SuccessEmailList,
                           "SUCCESS-loadmgr:{name}".format(name=args.job_name),
                           email_message_text,
                           message_html=email_message_html)
            except BaseException as be:
                logger.error("Failed to send email - {0!s}".format(be), exc_info=args.debug)
        # send end event to event api
        create_event(
            {
                "event_category": "info",
                "event_group": "LOAD MANAGER",
                "event_type": EVENT_TYPE,
                "event_subtype": "n/a",
                "event_name": job.job_name,
                "event_action": "end",
                "event_payload": {
                    "task_completed_count": task_completed_count,
                    "warning_count": warning_count
                }
            }
        )
    elif failure_count > 0:
        logger.error("Job had {0!s} failure(s)".format(failure_count))
        if warning_count:
            logger.warning("{0!s} task(s) completed with a warning".format(warning_count))
        jobrun.set_status(JOB_STATUS_FAILED)
        rc = 1
        # send end event to event api
        create_event(
            {
                "event_category": "error",
                "event_group": "LOAD MANAGER",
                "event_type": EVENT_TYPE,
                "event_subtype": "n/a",
                "event_name": job.job_name,
                "event_action": "end",
                "event_payload": {
                    "failure_count": failure_count,
                    "warning_count": warning_count
                }
            }
        )
    jobrun.end()
    logger.info("End {0}".format(program_name))
    sys.exit(rc)
except CLIError as e:
    # handle general errors in CLI
    try:
        jobrun.set_status(JOB_STATUS_FAILED)
    except:
        pass
    if 'logger' in globals().keys():
        logger.error(repr(e), exc_info=args.debug)
    else:
        sys.stderr.write(repr(e))
    # send end event to event api
    create_event(
        {
            "event_category": "error",
            "event_group": "LOAD MANAGER",
            "event_type": EVENT_TYPE,
            "event_subtype": "n/a",
            "event_name": job.job_name,
            "event_action": "end",
            "event_payload": {
                "error_message": repr(e)
            }
        }
    )
    sys.exit(1)
except Exception as e:
    try:
        jobrun.set_status(JOB_STATUS_FAILED)
    except:
        pass
    if 'logger' in globals().keys():
        logger.error("Job {0} caught an un-handled exception".format(args.job_name))
        logger.error(program_name + ": " + repr(e) + "\n")
        logger.debug("", exc_info=args.debug)
    else:
        sys.stderr.write("Job {0} caught an un-handled exception".format(args.job_name))
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
    # send end event to event api
    create_event(
        {
            "event_category": "critical",
            "event_group": "LOAD MANAGER",
            "event_type": EVENT_TYPE,
            "event_subtype": "n/a",
            "event_name": job.job_name,
            "event_action": "end",
            "event_payload": {
                "error_message": repr(e)
            }
        }
    )
    sys.exit(2)
finally:
    try:
        jobrun.set_value('job_pid',0)  # clear the job pid
    except:
        pass
