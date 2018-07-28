"""
loadmgr.cli -- Command-line client

loadmgr.cli is a command-line application for automating a series of steps for a specific job.

@author:     Jason DeCorte

@license:  Apache License 2.0

@contact:    jason.decorte@equifax.com

Version History:
0.01 jwd 8/28/2014
    Created sub-class GroupFileHandler to set umask on open for group writable file
0.02 jwd 9/10/2014
    Added code for list job function
    Added --patch optional parameter to "create" command for applying specific patches to the repository
    Updated to use new version of config now called gencfg.genericConfig 9/16
    Added export command and related function writeMetadata 9/17
0.03 jwd 9/24/2014
    Added environment config for child processes to inherit
    Moved environment config to beginning of app 9/30
    Added cfg parameter to repo initialization 9/30
    Removed genericConfig call to use imported config from __init__.py 9/30
0.04 jwd 10/17/2014
    Removed DEBUG and TESTRUN variables
    Changed log message for importing with FORCE option from warning to info 10/20
0.05 jwd 11/11/2014
    HOT FIX: Fix issue with Pause and Stop jobs - job_name must be set prior to calling updateJobStatus
    Added ability to stop ALL jobs
0.06 jwd 11/03/2014
    Add ability to start job and skip wait events or skip create events or specific events or all events
    Add stepRange type for optional step parameter for start job
    Updated Event management by adding new commands (add,disable,remove) 12/03
0.07 jwd 01/14/2015
    Skipped version since original load manager was already on version 0.7.
0.08 jwd 01/14/2015
    Changed name from efx_loader to loadmgr
    Merged loadmgr admin commands with this module
    Streamline of code using ORM
    Removed Main function - moved code to root level
    Removed old code
0.09 jwd 03/06/2015
    Removed patch functionality - separate script will be used for patching
    Updated version to be a string instead of float
    create_process now updates if process already exists
    start_job will send an email if job is already running
    store_metadata now updates job table if job exists and force mode is active
    Added job_group insert/update in store_metadata
    Added ability to start,stop or pause by job group
    Added listall opt arg to list only job runs with specific status
0.10 jwd 3/30/2015
    Fixed KeyError when creating heading for failure email if job is already running
0.11 jwd 4/16/2015
    Fixed error when creating a new process - UnboundLocalError("local variable 'process' referenced before assignment")
0.12 jwd 6/25/2015
    New versioning - master_version + __version__
    Added Adminuser check for command-line admin activities.  allows non-admins to execute some commands.
    Added currency_end_dttm_offset
    Added job_duration**
    Added rename command for process, currency and job
    Added change job group function for updating the job group
    Added temp_args parameter for start command
    Updated send_email to handle failure, warning and success emails
    Added create_email_body function
    Changed send_failure_email to call create_email_body function and pass email type of "f" (failure)
    Created send_warning_email to call create_email_body function and pass email type of "w" (warning)
    Created send_success_email to call create_email_body function and pass email type of "s" (success)
    Updated create_repository by removing the check for user in admin list - handled with argparse code
    Updated toggle_maintenance_mode by removing the check for user in admin list - handled with argparse code
0.13 jwd 8/5/2015
    HOTFIX: Removed the defaults from the Process Update command
    HOTFIX: Changed the condition in update_process to "is not None" since some valid values (0) are false
0.14 jwd 8/14/2015
    HOTFIX: Updated the check on function name to create_repository in the repository version condition check
0.15 jwd 10/23/2015
    Changed to use Python2.7 interpreter
    Added exception check peewee.ImproperlyConfigured for repository version check
0.16 jwd 11/30/2015
    Changed the versioning numbering
    Added read_job_file for future support of JSON formatted job file
    Updated function docstrings
0.17 jwd 01/11/2016
    Added OperationalError to TRY/EXCEPT on RepoHist
    Fix import job file issue when it contains comment header by creating new function load_csv (instead peewee's)
0.18 jwd3 01/28/2016
    Removed initialize environment using env.cfg.  Sourcing env.ksh in the wrapper script eliminates it.
    Added path to subprocess call of efxldr
0.19 jwd3 02/14/2016
    HOTFIX: Added job reset command to set status to not running for jobs that crashed and still had Running status
0.20 jwd3 03/16/2016
    HOTFIX: Updated start_job to use new load_job_file_csv function instead of peewee's load_csv
0.21 jwd3 7/21/2016
    HOTFIX: Added check for ingore error flag and capture output flag
    HOTFIX: Added email notification for critical failure not connecting to repo
0.22 jwd3 7/22/2016
    HOTFIX: Missed the word "start" in the restart command of the critical email message
    HOTFIX: Updated variable msg to email_msg to avoid conflict with variable used in functions
    HOTFIX: Added critical emails to failure email recipient list
0.23 jwd3 10/07/2016 HOTFIX
    Added enabled indicator to event list
    Moved check on function name - only get repo hist if not create_repository
0.24 jwd3 02/16/2017 Maintenance Release
    Fixed issue with job files having fields containing the delimiter character
    Re-organize modules
    removed send_success_email
    Added args parameter to each function
    Moved main into a function called main for import into generated script
0.25 jwd3 02/22/2017 HOTFIX
    Added check that steps still exist to resume; if not log a warning.
    Changed program version - removed concatenate with project version
    Fixed bug with job reset when pid is 0
0.26 jwd3 03/21/2017 Bug Fix
    Updated send_critical_email's call to send_email - had two "args" parameters
0.27 jwd3 04/13/2017 v1.2.4
    Moved send_email function to utils.emailer - now supports file attachments
    Renamed old send_email to send_email_prep
0.28 jwd3 04/21/2017 v.1.3.0 BugFix
    Added check if on resume previous start had steps overridden and set end step correctly
0.29 jwd3 05/11/2017 v2.0.0
    Versioning changed to 0.xx where xx is sequential number representing each change to this file
        advanced versioning with major/minor isn't needed
    BugFix - Export of metadata was missing escapechar option for csv writer
    Added ability to delete jobs - prompt for confirmation
0.30 jwd3 05/22/2017 v2.1.2
    Updated build date to come from __init__
0.31 jwd3 06/15/2017 v2.2.0
    Added option to suppress success emails
"""
import argparse
import csv
import json
import logging
import os
import subprocess
import sys
from ast import literal_eval
from datetime import datetime

import jsonschema
import pkg_resources
from peewee import ImproperlyConfigured, OperationalError
from playhouse.csv_loader import dump_csv

from loadmgr.job import __version__ as job_version
from loadmgr.utils.decorators import LogEntryExit
from loadmgr.utils.decorators import __version__ as decorators_version
from loadmgr.metadata.constants import EVENT_ACTION_TYPE,EVENT_TYPE,EVENT_ACTION_WAIT,EVENT_TYPE_FILE,JOB_STATUS_READY
from loadmgr.metadata.constants import JOB_STATUS_NOT_RUNNING,JOB_STATUS_COMPLETED
from loadmgr.metadata.constants import JOB_STATUS_RUNNING,JOB_STATUS_WAITING,JOB_STATUS_STOPPED,JOB_STATUS_FAILED
from loadmgr.metadata.constants import LOGGING_FORMAT,GENERIC_LOADER,JOB_STATUS_SIP,JOB_STATUS_PIP,JOB_STATUS_PAUSED
from loadmgr.metadata.constants import PKGNAME,STATIC_DIR,JOB_FILE_JSON_SCHEMA,JOB_FILE_CSV_STEP_SCHEMA
from loadmgr.metadata.constants import PROCESS_TYPE,PROCESS_TYPE_INGESTION,EMAIL_MESSAGE_HTML,EMAIL_MESSAGE_TEXT
from loadmgr.metadata.constants import __version__ as constants_version
from loadmgr.metadata.models import __version__ as models_version
from loadmgr import cfg as config
from loadmgr import master_version,repository_version,adminuser, __updated__ as build_date
from loadmgr.utils.emailer import send_email

#===============================================================================

#__all__ = [] # list all user defined libraries here
__version__ = "0.31"
__date__ = '2014-08-08'
__updated__ = '06/15/2017'

program_name = os.path.basename(sys.argv[0])
program_version = "v{0}".format(__version__)
project_version = "v%s" % master_version
program_build_date = str(build_date)
#program_version_message = 'Project %s / %%(prog)s %s (%s)' % (project_version,program_version, program_build_date)
program_version_message = '''Project       {0}
{8:<14}{1}
Repository    v{2}
Build Date    {3}
Modules:
   Job        v{4}
   Models     v{5}
   Constants  v{6}
   Decorators v{7}
'''.format(project_version,program_version,repository_version,program_build_date,
           job_version,models_version,constants_version,decorators_version,program_name)
program_shortdesc = __doc__.split("\n")[1]
program_license = '''%s

  Created by Jason DeCorte on %s.

  Licensed under the Apache License 2.0
  http://www.apache.org/licenses/LICENSE-2.0

  Distributed on an "AS IS" basis without warranties
  or conditions of any kind, either express or implied.

USAGE
''' % (program_shortdesc, str(__date__))

#initialize logging
logger = logging.getLogger(program_name.split(".")[0])
logger.setLevel(logging.DEBUG)  # set to lowest level - fine control at handler
formatter = logging.Formatter(LOGGING_FORMAT)

# Moved after logger initialization since module also contains logging
from metadata.models import *


class GroupFileHandler(logging.FileHandler):
    """ Sub-class to override the open method for group writable log file """
    def _open(self):
        prevumask = os.umask(0o002)
        stream = logging.FileHandler._open(self)
        os.umask(prevumask)
        return stream


def test_temp_args(arg):
    """
    Test the input for the temporary args
    :param arg:
    :return: keyword dictionary
    """
    #--tempargs=load_strategy:manual start_date:"2015-06-25 13:43:00" end_date:"2015-06-25 13:43:00"
    # temp_args=[['load_strategy', 'full'], ['start', '2015-07-01']]
    if arg.count(":") == 0:
        msg = "Invalid format - must be in the format key:value key2:value2 ..."
        raise argparse.ArgumentTypeError(msg)
    return arg.split(":",1)


def step_range(step):
    """
    Test the input parameter for proper values in the step range
    :param step:
    :return:
    """
    step_range_list = []
    if step.count('-') > 0:
        #step contains a range
        for s in step.split('-'):
            if s and s.isdigit():
                step_range_list.append(int(s))
            elif not s:
                step_range_list.append(-1)
            else:
                msg = "Invalid value for optional step parameter [{0}]".format(step)
                raise argparse.ArgumentTypeError(msg)
        if len(step_range_list) != 2:
            msg = ("Step range contains more than 2 step values [{0!s}]"
                   .format(','.join(map(lambda x: str(x),step_range_list))))
            raise argparse.ArgumentTypeError(msg)
        if -1 < step_range_list[1] < step_range_list[0]:
            msg = ("Invalid step range; start step should be less than or equal to end step [{0!s}]"
                   .format('-'.join(map(lambda x: str(x),step_range_list))))
            raise argparse.ArgumentTypeError(msg)
    else:
        if step.isdigit():
            step_range_list = [int(step)] * 2
        else:
            msg = "Invalid value for optional step parameter [{0}]".format(step)
            raise argparse.ArgumentTypeError(msg)
    return step_range_list


@LogEntryExit(logger)
def load_job_file_csv(args,model_class,csv_file,comment_char='#',delimiter=','):
    """
    Load a Job File in csv format with header recorder into the model class ignoring lines starting with # (comments)
    :param model_class: PeeWee Model Class defining the fields
    :param csv_file: delimited text file with same fields as defined in Model Class
    :param args: command-line arguments
    :param comment_char: character used to indicate a line is a comment
    :param delimiter: field delimiter in csv file; defaults to comma
    :return: PeeWee Query to select all rows
    """
    def convert_to_boolean(value):
        """
        Convert string value to boolean value
        :param value: String value represented a boolean value
        :return: boolean value
        """
        try:
            return literal_eval(value.capitalize())
        except:
            logger.error("Invalid boolean value [{0}]".format(value))
            return None
    logger.info("Open Job File [{0}]".format(csv_file))
    jobfile_fp = open(csv_file,'r')
    logger.info("Parse job file in records of columns")
    records = []
    try:
        # Convert each row that doesn't start with comment character to a dictionary using header record for names
        jf_reader = csv.DictReader((x for x in jobfile_fp if not x.startswith(comment_char)),
                                   restkey="extra",
                                   restval=None,
                                   escapechar='\\',
                                   delimiter=delimiter)
    except csv.Error as csv_em:
        logger.error('Failed to read Job File [{0!r}]'.format(csv_em), exc_info=args.debug)
        return None
    field_count = len(jf_reader.fieldnames)
    if field_count == 0:
        logger.error("Job File is missing header record")
        return None
    job_file_schema = pkg_resources.resource_string(PKGNAME,os.path.join(STATIC_DIR,JOB_FILE_CSV_STEP_SCHEMA))
    schema = json.loads(job_file_schema)
    for row in jf_reader:
        if row.get('job_step_ignore_error'):
            row['job_step_ignore_error'] = convert_to_boolean(row['job_step_ignore_error'])
        if row.get('job_step_capture_output'):
            row['job_step_capture_output'] = convert_to_boolean(row['job_step_capture_output'])
        try:
            jsonschema.validate(row,schema)
        except jsonschema.ValidationError as sm_error_msg:
            logger.error("Line {0!s} - {1}".format(jf_reader.line_num,sm_error_msg),exc_info=args.debug)
            return None
        else:
            logger.info("Job file step {0} validated".format(row['job_step']))
            records.append(row)
    logger.info("Create table")
    try:
        model_class.create_table()
    except:
        logger.error("Failed to create in-memory table for job file", exc_info=args.debug)
        return None
    logger.info("Insert records from file excluding records with non-digit job step")
    try:
        model_class.insert_many([x for x in records if x['job_step'].isdigit()]).execute()
    except:
        logger.error("Failed on Insert into in-memory Job File table", exc_info=args.debug)
        return None
    return model_class.select()


@LogEntryExit(logger)
def send_email_prep(args, message_text, message_html=None, email_type="f"):
    """
    Create appropriate subject and call send_email function
    :param message_text:
    :param args: Command-line Arguments
    :param message_html:
    :param email_type:
    :return:
    """
    email_status = {
        'f':'FAILURE',
        'w':'WARNING',
        's':'SUCCESS',
        'c':'CRITICAL ERROR',
    }
    recipients = config.FailureEmailList if email_type in ["f","w","c"] else config.SuccessEmailList
    status = email_status.get(email_type)
    subject = "{status}-loadmgr:{name}".format(status=status,name=args.job_name or '')
    logger.debug("Subject: {0}".format(subject))
    try:
        send_email(recipients,subject,message_text,message_html=message_html)
    except BaseException as e:
        logger.error("Failed to send email - {0!s}".format(e), exc_info=args.debug)
        return 1
    else:
        logger.debug("Email sent")
        return 0


@LogEntryExit(logger)
def create_email_body(args,heading,job_details,email_type):
    """
    create email body
    :param args: command-line arguments
    :param heading:
    :param job_details:
    :param email_type:
    :return:
    """
    if job_details:
        email_message_text = EMAIL_MESSAGE_TEXT.format(heading=heading,
                                                       job_status=job_details.job_status,
                                                       job_start_dttm=job_details.job_start_dttm,
                                                       job_pid=job_details.job_pid,
                                                       job_log_file=job_details.job_log_file,
                                                       job_start_step=job_details.job_start_step,
                                                       job_end_step=job_details.job_end_step,
                                                       job_last_completed_step=job_details.job_last_completed_step)
        email_message_html = EMAIL_MESSAGE_HTML.format(heading=heading,
                                                       job_status=job_details.job_status,
                                                       job_start_dttm=job_details.job_start_dttm,
                                                       job_pid=job_details.job_pid,
                                                       job_log_file=job_details.job_log_file,
                                                       job_start_step=job_details.job_start_step,
                                                       job_end_step=job_details.job_end_step,
                                                       job_last_completed_step=job_details.job_last_completed_step)
    else:
        email_message_text = EMAIL_MESSAGE_TEXT.format(heading=heading,
                                                       job_status="",
                                                       job_start_dttm="",
                                                       job_pid="",
                                                       job_log_file="",
                                                       job_start_step="",
                                                       job_end_step="",
                                                       job_last_completed_step="")
        email_message_html = EMAIL_MESSAGE_HTML.format(heading=heading,
                                                       job_status="",
                                                       job_start_dttm="",
                                                       job_pid="",
                                                       job_log_file="",
                                                       job_start_step="",
                                                       job_end_step="",
                                                       job_last_completed_step="")
    return send_email_prep(args, email_message_text, email_message_html, email_type=email_type)


@LogEntryExit(logger)
def send_failure_email(args,heading,job_details=None):
    """
    Send failure email
    :param args: command-line arguments
    :param heading:
    :param job_details:
    :return:
    """
    return create_email_body(args,heading,job_details,email_type="f")


@LogEntryExit(logger)
def send_warning_email(args,heading,job_details=None):
    """
    Send warning email
    :param args: command-line arguments
    :param heading:
    :param job_details:
    :return:
    """
    return create_email_body(args,heading,job_details,email_type="w")


# @LogEntryExit(logger)
# def send_success_email(heading,job_details=None):
#     """
#     Send success email
#     :param heading:
#     :param job_details:
#     :return:
#     """
#     return create_email_body(heading,job_details,email_type="s")


@LogEntryExit(logger)
def send_critical_email(message,args):
    """
    Send critical email
    :param message:
    :param args: Command-line Arguments
    :return:
    """
    # return create_email_body(heading,job_details,email_type="s")
    if args.job_name:
        restart_text = "To Restart the job:<br>$PSOL_HOME/loadmgr/loadmgr -S start {0}".format(args.job_name)
    else:
        restart_text = ""
    email_message_text = "ERROR!ERROR!ERROR\n{0}".format(message)
    email_message_html = """
    <html><head></head>
    <body>
    <h1>CRITICAL ERROR</h1>
    <h3>{msg}</h3>
    <p>{restart}</p>
    </body>
    </html>
    """.format(msg=message,restart=restart_text)
    return send_email_prep(args, email_message_text, email_message_html, email_type='c')


@LogEntryExit(logger)
def read_job_file(args):
    logger.info("Load the steps from the job file")
    try:
        job_file_dict = json.load(args.job_file)
    except ValueError as sm_error_msg:
        logger.error("Failed to parse data\n{0!s}".format(sm_error_msg),exc_info=args.debug)
        return None
    except:
        logger.error("Invalid file",exc_info=args.debug)
        return None
    job_file_schema = pkg_resources.resource_string(PKGNAME,os.path.join(STATIC_DIR,JOB_FILE_JSON_SCHEMA))
    schema = json.loads(job_file_schema)
    try:
        jsonschema.validate(job_file_dict,schema)
    except jsonschema.ValidationError as sm_error_msg:
        logger.error(sm_error_msg,exc_info=args.debug)
        return None
    else:
        logger.info("Job file validated")
    return job_file_dict


# noinspection PyUnresolvedReferences
@LogEntryExit(logger)
def start_job(args):
    """ Start a job """
    logger.info("Start job {0}".format(args.job_name))
    logger.info("Checking if job exists in the repository")
    try:
        single_job = Job.get_job(args.job_name)
        logger.debug("Convert single job to a list")
        job_list = [single_job]
    except Job.DoesNotExist:
        logger.debug("Check if job group was passed")
        job_list = Job.get_job_group(args.job_name)
    except:
        logger.error("Failed to get job data from repository", exc_info=args.debug)
        return 1
    if not job_list:
        logger.error("Job or Job Group {0} does NOT exist in repository".format(args.job_name), exc_info=args.debug)
        return 1
    logger.debug("Count of jobs= {0!s}".format(len(job_list)))
    logger.debug("Execute for each job in job_list")
    job_failure_count = 0
    for job in job_list:
        logger.info("Processing job {0}".format(job.job_name))
        logger.debug("Check for existing job run")
        try:
            jobrun = JobRun.get_job_run(job)
        except JobRun.DoesNotExist:
            logger.info("No existing job run; creating new entry")
            try:
                jobrun = JobRun.add_job_run(job)
            except:
                logger.error("Failed to create new job run entry for job {0}".format(job.job_name), exc_info=args.debug)
                if args.silent:
                    logger.debug("Send email to warn user that job run creation failed")
                    heading = "Failed to create new job run entry for job {0}".format(job.job_name)
                    send_failure_email(args,heading)
                job_failure_count += 1
                continue
        except:
            logger.error("Failed to get job run data for job {0}".format(job.job_name), exc_info=args.debug)
            if args.silent:
                logger.debug("Send email to warn user that job run data retrieval failed")
                heading = "Failed to get job run data for job {0}".format(job.job_name)
                send_failure_email(args,heading)
            job_failure_count += 1
            continue
        if jobrun.job_status in (JOB_STATUS_RUNNING,JOB_STATUS_SIP,JOB_STATUS_PIP,JOB_STATUS_WAITING):
            logger.error("Job {0} is already running".format(job.job_name), exc_info=args.debug)
            if args.silent:
                logger.debug("Send email to warn user that job is already running")
                heading = "Job {0} is already running!".format(job.job_name)
                send_warning_email(args,heading,jobrun)
            job_failure_count += 1
            continue
        elif jobrun.job_status == JOB_STATUS_PAUSED:
            logger.info("Job {0} was un-paused".format(job.job_name))
            JobRunFlag.unpause(jobrun)
            logger.info("Reset status to ready for job {0}".format(job.job_name))
            jobrun.set_status(JOB_STATUS_READY)
            logger.debug("PID= {0!s}".format(jobrun.job_pid))
            continue
        elif jobrun.job_status in (JOB_STATUS_NOT_RUNNING,JOB_STATUS_COMPLETED,JOB_STATUS_STOPPED,JOB_STATUS_FAILED):
            if jobrun.job_status in (JOB_STATUS_COMPLETED,JOB_STATUS_STOPPED,JOB_STATUS_FAILED):
                logger.info("Save previous run to history tables")
                if not save_previous_run(jobrun):
                    if args.silent:
                        logger.debug("Send email to warn user of failure to save previous job run")
                        heading = "Failed to save previous job run to history tables for job {0}".format(job.job_name)
                        send_failure_email(args,heading,jobrun)
                    job_failure_count += 1
                    continue
            if jobrun.job_status in (JOB_STATUS_STOPPED,JOB_STATUS_FAILED) and args.resume:
                if args.step[0] < 0:  # check if start step was overridden on CL;if not set to next after last completed
                    logger.info("Set start step to next step after last completed step")
                    job_steps_left = [x for x in jobrun.job_queue.keys() if x > jobrun.job_last_completed_step]
                    if job_steps_left:
                        args.step[0] = min(job_steps_left)
                        logger.info("{0} step(s) left to run".format(len(job_steps_left)))
                    else:
                        logger.warn("NO steps left to run; Cannot resume job!")
                        job_failure_count += 1
                        continue
                    logger.info("Set job to skip all wait events on resume")
                    args.skipEventList.append(EVENT_ACTION_WAIT)  # on resume, skip all wait events
                if args.step[1] < 0:  # end step not overridden
                    args.step[1] = jobrun.job_end_step  # set to current end step in case steps overridden
            if args.job_file:
                logger.info("Using job step data from file passed on command line")
                task_query = load_job_file_csv(args,
                                               JobFile,
                                               args.job_file,
                                               comment_char='#',
                                               delimiter=config.jobFileDelimiter.encode())
            else:
                logger.info("Using job step data from repository")
                task_query = JobStep.get_export_query(job)
            event_query = job.events.where(JobEvent.event_active_ind == True)
            try:
                jobrun.prepare_run(task_query,event_query,args.step,args.skipEventList,dict(args.temp_args))
            except:
                logger.error("Failed to prepare job run for job {0}".format(job.job_name), exc_info=args.debug)
                if args.silent:
                    logger.debug("Send email to warn user of failure to prepare job run")
                    heading = "Failed to prepare job run for job {0}".format(job.job_name)
                    send_failure_email(args,heading,jobrun)
                job_failure_count += 1
                continue
            logger.info("start generic loader")
            optional_flags = []
            if args.debug:
                optional_flags.append("--debug")
            if args.no_success_email:
                optional_flags.append("--no-success-email")
            # loadmgr_home = os.getenv("LOADMGR_HOME")
            # logger.debug("LOADMGR_HOME={0}".format(loadmgr_home))
            process_args = [GENERIC_LOADER,job.job_name] + optional_flags
            #proc = subprocess.Popen(process_args)
            # uncomment for production so stdout and stderr are redirected
            proc = subprocess.Popen(process_args,stdout=open(r"/dev/null",'a+'),stderr=subprocess.STDOUT)
            jobrun.set_value('job_pid',proc.pid)
            logger.info("{0} started as PID {1!s} for job {2}".format(GENERIC_LOADER,jobrun.job_pid,job.job_name))
        else:  # JOB_STATUS_READY
            logger.warn("Job is already in Ready state")
    if job_failure_count == len(job_list):
        logger.error("All jobs failed to start or were already started")
        return 1
    elif job_failure_count:
        logger.warning("{0!s} job(s) failed to start or were already started".format(job_failure_count))
        return 2
    else:
        logger.info("All jobs were started")
        return 0


@LogEntryExit(logger)
def stop_job(args):
    """ Stop a job """
    if args.job_name.upper() == 'ALL':
        logger.info("Stopping ALL running jobs")
        run_list = [x for x in JobRun.get_job_run_list() if x.job_status == JOB_STATUS_RUNNING]
    else:
        logger.info("Stopping the job or job group {0}".format(args.job_name))
        logger.info("Loading job run data from repository")
        run_list = [x for x in JobRun.get_job_run_list() if x.job_key.job_name == args.job_name]
        if not run_list:
            logger.warning("Job not found; checking for job group")
            run_list = [x for x in JobRun.get_job_run_list() if x.job_key.job_group == args.job_name]
        if not run_list:
            logger.error("Job or Job Group {0} does NOT exist".format(args.job_name))
            return 1
    for jobrun in run_list:
        JobRunFlag.stop(jobrun)
        logger.info("Job run stop flag set for job {0}".format(jobrun.job_key.job_name))
    return 0


@LogEntryExit(logger)
def pause_job(args):
    """ Pause a job """
    if args.job_name.upper() == 'ALL':
        logger.info("Pausing ALL running jobs")
        run_list = [x for x in JobRun.get_job_run_list() if x.job_status == JOB_STATUS_RUNNING]
    else:
        logger.info("Pausing the job or job group {0}".format(args.job_name))
        logger.info("Loading job run data from repository")
        run_list = [x for x in JobRun.get_job_run_list() if x.job_key.job_name == args.job_name]
        if not run_list:
            logger.warning("Job not found; checking for job group")
            run_list = [x for x in JobRun.get_job_run_list() if x.job_key.job_group == args.job_name]
        if not run_list:
            logger.error("Job or Job Group {0} does NOT exist".format(args.job_name))
            return 1
    for jobrun in run_list:
        JobRunFlag.pause(jobrun)
        logger.info("Job run pause flag set for job {0}".format(jobrun.job_key.job_name))
    return 0


@LogEntryExit(logger)
def rename_job(args):
    """
    Rename an event for a job
    """
    logger.info("Rename job {0}".format(args.job_name_old))
    logger.info("Checking if job exists in the repository")
    job = Job.get_job(args.job_name_old)
    if job:
        logger.info("Job found.")
        job.job_name = args.job_name_new
        job.save()
    else:
        logger.error("Job {0} does NOT exist".format(args.job_name_old), exc_info=args.debug)
        return 1
    logger.info("Job renamed to {0}".format(args.job_name_new))
    return 0


@LogEntryExit(logger)
def change_job_group(args):
    """
    Change job group for a job
    """
    logger.info("Change job group for job {0}".format(args.job_name))
    logger.info("Checking if job exists in the repository")
    job = Job.get_job(args.job_name)
    if job:
        logger.info("Job found.")
        job.job_group = args.group_name
        job.save()
    else:
        logger.error("Job {0} does NOT exist".format(args.job_name), exc_info=args.debug)
        return 1
    logger.info("Job group changed to {0}".format(args.group_name))
    return 0


@LogEntryExit(logger)
def reset_job_status(args):
    """
    Reset job status to not running
    """
    def check_process_existence(pid):
        """
        Check if the passed pid exists in the system
        :param pid: Pid of the process to check existence of
        :return: True or False
        """
        if pid == 0:
            return False
        try:
            os.getpgid(pid)
        except:
            return False
        else:
            return True
    logger.info("Reset job status for job {0}".format(args.job_name))
    logger.info("Checking if job exists in the repository")
    job = Job.get_job(args.job_name)
    if not job:
        logger.error("Job {0} does NOT exist".format(args.job_name), exc_info=args.debug)
        return 1
    else:
        logger.info("Job found.")
        try:
            jobrun = JobRun.get_job_run(job)
        except JobRun.DoesNotExist:
            logger.warn("No existing job run; Job status not reset")
            return 0
        except:
            logger.error("Failed to get job run data for job {0}".format(job.job_name), exc_info=args.debug)
            return 1
        if check_process_existence(jobrun.job_pid):
            logger.error("Cannot reset the status of a job still executing!")
            return 1
        jobrun.job_status = JOB_STATUS_NOT_RUNNING
        jobrun.job_pid = 0
        jobrun.save()
        logger.info("Status reset for job {0}".format(args.job_name))
        return 0


@LogEntryExit(logger)
def remove_job(args):
    """
    Remove an existing job, job steps and job history.

    NOTE: Also removes history in all the job tables!!!
    """
    # Get confirmation
    prompt = 'WARNING: Removing a job is PERMANENT and cannot be undone!!\nAre you sure want to remove the job [{0}]? '
    response = raw_input(prompt.format(args.job_name))
    if response.lower() in ['yes','y']:
        logger.info("Removing job {0}".format(args.job_name))
        try:
            job = Job.get_job(args.job_name)
            job.remove()
            logger.info("Job [{0}] successfully removed".format(args.job_name))
        except Process.DoesNotExist:
            logger.error("Job [{0}] does not exist in repository".format(args.job_name))
            return 1
        except:
            logger.error("Failed to remove job ", exc_info=args.debug)
            return 1
    else:
        logger.info("ABORTED - Job [{0}] was NOT removed.".format(args.job_name))
    return 0


@LogEntryExit(logger)
def list_jobs(args):
    """
    List all the jobs and some of their info in the repository
    """
    logger.info("list all job runs in the repository")
    try:
        run_list = Job.get_job_list_with_info()
    except:
        logger.error("Failed to get run list", exc_info=args.debug)
        return 1
    if args.status:
        logger.info("Filter job run list for job status {0}".format(args.status))
        run_list = [x for x in run_list if x.job_run.job_status == args.status]
    logger.info("{0:3}  {1:<40} {2:<20} {3:<19} {4:<19}".format(" ","Name","Status","Start","End"))
    for num,run in enumerate(run_list,1):
        sfmt = '%Y-%m-%d %H:%M:%S' if run.job_run.job_start_dttm else "<20"
        efmt = '%Y-%m-%d %H:%M:%S' if run.job_run.job_end_dttm else "<20"
        logger.info("{0:3}. {1.job_name:<40} {1.job_run.job_status:<20} {1.job_run.job_start_dttm:{2}} "
                    "{1.job_run.job_end_dttm:{3}}".format(num,run,sfmt,efmt))
    return 0


@LogEntryExit(logger)
def write_metadata(args):
    """
    Write the job task/step metadata to a job file
    """
    logger.info("Writing list of steps for job {0} to file".format(args.job_name))
    logger.info("Loading job from repository")
    job = Job.get_job(args.job_name)
    if job:
        jobfile = csv.writer(args.job_file, delimiter=config.jobFileDelimiter.encode(),lineterminator=os.linesep,
                             quoting=csv.QUOTE_NONE, quotechar="'", escapechar='\\')
        try:
            query = JobStep.get_export_query(job)
            dump_csv(query, args.job_file, append=False, csv_writer=jobfile)
        except:
            logger.error("Failed to write the metadata to the job file", exc_info=args.debug)
            return 1
    else:
        logger.error("Job {0} does NOT exist".format(args.job_name))
        return 1
    logger.info("Job steps successfully exported to file {0}".format(args.job_file.name))
    return 0


@LogEntryExit(logger)
def store_metadata(args):
    """
    Store job metadata in database
    """
    logger.info("Import metadata for job {0}".format(args.job_name))
    logger.info("Load the steps from the job file")

    job_file = load_job_file_csv(args,JobFile,args.job_file,comment_char='#',delimiter=config.jobFileDelimiter.encode())
    if not job_file:
        return 1
    logger.info("Checking if job already exists in the repository")
    try:
        job = Job.get_job(args.job_name)
    except Job.DoesNotExist:
        logger.info("Job NOT found and will be added")
        job = None
    if job:
        logger.info("Job already exists")
        if args.force:
            logger.info("Remove existing steps")
            try:
                steps_deleted_count = JobStep.remove(job)
                logger.debug("Deleted {0!s} records".format(steps_deleted_count))
            except:
                logger.error("Failed to delete existing steps", exc_info=args.debug)
                return 1
            if not steps_deleted_count:
                logger.warning("No existing job steps found to delete")
            logger.debug("Add new steps")
            try:
                JobStep.load(job, job_file)
                logger.info("Steps successfully loaded")
            except:
                logger.error("Failed to load job steps into repository", exc_info=args.debug)
                return 1
            try:
                logger.info("Updating job")
                job.job_file = args.job_file
                job.job_group = args.job_group
                job.save()
            except:
                logger.error("Failed to update job", exc_info=args.debug)
                return 1
        else:
            logger.warning("Job {0} already exists; Metadata NOT updated; Use FORCE mode to update existing jobs"
                           .format(args.job_name))
            return 0
    else:
        job = Job.add_job(args.job_name, args.job_file, args.job_group)
        logger.info("New job added")
        try:
            JobStep.load(job, job_file)
            logger.info("Steps successfully loaded")
        except ValueError as err:
            logger.error("ValueError: {0}".format(err))
            job.delete_instance()  # Job instance removed since new and steps failed to load
            return 1
        except:
            logger.error("Failed to load job steps into repository", exc_info=args.debug)
            job.delete_instance()  # Job instance removed since new and steps failed to load
            return 1
        
    logger.info("Metadata for job {0} successfully stored".format(args.job_name))
    return 0


@LogEntryExit(logger)
def add_event(args):
    """
    Add event to a job
    """
    logger.info("Adding event {1} with job {2} - [{0}]".format(args.event_item,args.event_name,args.job_name))
    logger.info("Checking if job exists in the repository")
    job = Job.get_job(args.job_name)
    if job:
        logger.info("Job found.")
        logger.info("Check if event already exists")
        if args.event_name in [event.event_name for event in JobEvent.get_event_list(job)]:
            logger.warning("Event {0} already exists for job {1}".format(args.event_name,args.job_name))
            return 1
        try:
            JobEvent.add(job,args.event_name,args.event_action_type,args.event_type,args.event_item)
        except:
            logger.error("Failed to add event to the job", exc_info=args.debug)
            return 1
    else:
        logger.error("Job [{0}] does NOT exist".format(args.job_name))
        return 1
    logger.info("Event for job {0} successfully added".format(args.job_name))
    return 0


@LogEntryExit(logger)
def disable_event(args):
    """
    Disable event(s) for a job
    """
    logger.info("Disable these events [{0}] to the job {1}".format(','.join(args.event_name),args.job_name))
    logger.info("Checking if job exists in the repository")
    job = Job.get_job(args.job_name)
    if job:
        logger.info("Job found.")
        try:
            JobEvent.set_active_ind(job,args.event_name,False)
        except:
            logger.error("Failed to disable event(s) [{0}] for job {1}".format(','.join(args.event_name),args.job_name),
                         exc_info=args.debug)
            return 1
    else:
        logger.error("Job {0} does NOT exist".format(args.job_name))
        return 1
    logger.info("Event(s) [{0}] for job {1} successfully disabled".format(','.join(args.event_name),args.job_name))
    return 0


@LogEntryExit(logger)
def enable_event(args):
    """
    Enable event(s) for a job
    """
    logger.info("Enable these events [{0}] to the job {1}".format(','.join(args.event_name),args.job_name))
    logger.info("Checking if job exists in the repository")
    job = Job.get_job(args.job_name)
    if job:
        logger.info("Job found.")
        try:
            JobEvent.set_active_ind(job,args.event_name,True)
        except:
            logger.error("Failed to enable event(s) [{0}] for job {1}".format(','.join(args.event_name),args.job_name),
                         exc_info=args.debug)
            return 1
    else:
        logger.error("Job {0} does NOT exist".format(args.job_name))
        return 1
    logger.info("Event(s) [{0}] for job {1} successfully enabled".format(','.join(args.event_name),args.job_name))
    return 0


@LogEntryExit(logger)
def remove_event(args):
    """
    Remove event(s) for a job
    """
    logger.info("Remove event(s) [{0}] to the job {1}".format(','.join(args.event_name),args.job_name))
    logger.info("Checking if job exists in the repository")
    job = Job.get_job(args.job_name)
    if job:
        logger.info("Job found.")
        try:
            JobEvent.remove(job,args.event_name)
        except:
            logger.error("Failed to remove event(s) [{0}] for job {1}".format(','.join(args.event_name),args.job_name),
                         exc_info=args.debug)
            return 1
    else:
        logger.error("Job {0} does NOT exist".format(args.job_name))
        return 1
    logger.info("Event(s) [{0}] for job {1} successfully removed".format(','.join(args.event_name),args.job_name))
    return 0


@LogEntryExit(logger)
def rename_event(args):
    """
    Rename an event for a job
    """
    logger.info("Rename event {0} for the job {1}".format(args.event_name_old,args.job_name))
    logger.info("Checking if job exists in the repository")
    job = Job.get_job(args.job_name)
    if job:
        logger.info("Job found.")
        logger.info("Get corresponding event info")
        try:
            job_event = job.events.where(JobEvent.event_name == args.event_name_old).get()
        except JobEvent.DoesNotExist:
            logger.error("Event {0} does not exist for job {1]".format(args.event_name_old,args.job_name),
                         exc_info=args.debug)
            return 1
        except:
            logger.error("Failed to rename event {0} for job {1}".format(args.event_name_old,args.job_name),
                         exc_info=args.debug)
            return 1
        job_event.event_name = args.event_name_new
        job_event.save()
    else:
        logger.error("Job {0} does NOT exist".format(args.job_name), exc_info=args.debug)
        return 1
    logger.info("Event {0} for job {1} successfully renamed".format(args.event_name_new,args.job_name))
    return 0


@LogEntryExit(logger)
def view_event(args):
    """
    View event(s) for a job
    """
    logger.info("View event(s) for the job {0}".format(args.job_name))
    logger.info("Checking if job exists in the repository")
    job = Job.get_job(args.job_name)
    if job:
        logger.info("Job found.")
        logger.info("Event List:")
        logger.info("=" * 40)
        event_list = JobEvent.get_event_list(job)
        if not event_list:
            logger.info("NONE")
            logger.info("=" * 40)
        else:
            for num,event in enumerate(event_list,1):
                logger.info("{0:2}. Name: {1.event_name}".format(num,event))
                logger.info("{0:2}. Action Type: {1.event_action_type}".format(num,event))
                logger.info("{0:2}. Event Type: {1.event_type}".format(num,event))
                logger.info("{0:2}. Event: {1}".format(num,event.event_file_name or event.event_function))
                logger.info("{0:2}. Enabled: {1.event_active_ind}".format(num,event))
                logger.info("=" * 40)
    else:
        logger.error("Job {0} does NOT exist".format(args.job_name))
        return 1
    logger.info("Event list for job {0} completed".format(args.job_name))
    return 0


@LogEntryExit(logger)
def create_repository():
    """
    Create repository objects in a database or apply patch to existing repo
    """
    logger.info("Creating job repository")
    return_code = create_tables()
    if return_code:
        logger.info("Repository created successfully")
        return 0
    else:
        return 1


@LogEntryExit(logger)
def toggle_maintenance_mode(args):
    """
    Toggle maintenance mode
    """
    logger.info("Toggle maintenance mode on/off")
    try:
        repository = RepoHist.get()
        repository.maintenance_mode_flg = not repository.maintenance_mode_flg
        repository.save()
    except:
        logger.error("Failed to toggle maintenance mode", exc_info=args.debug)
        return 1
    logger.info("Repository is{0} in maintenance mode".format("" if repository.maintenance_mode_flg else " NOT"))
    return 0


@LogEntryExit(logger)
def create_process(args):
    """
    Create a new process
    """
    logger.info("Create a new process called {0}".format(args.process_name))
    logger.info("Check if process already exists")
    try:
        process = Process.get_process(args.process_name)
        logger.error("Process {0} already exists and will be updated".format(process.process_name))
        return 1
    except Process.DoesNotExist:
        logger.info("Process {0} does not already exist in repository".format(args.process_name))
    except:
        logger.error("Failed to determine if process already exists in repository", exc_info=args.debug)
        return 1
    process = Process.add_process(args.process_name,
                                  args.processType,
                                  args.subjectArea,
                                  args.rowCountType,
                                  args.rowCountProperty,
                                  args.currencyDateFlag,
                                  args.currencyStartOffset,
                                  args.currencyEndOffset)
    logger.info("{0} process successfully".format(process.process_name))
    return 0


@LogEntryExit(logger)
def update_process(args):
    """
    Update an existing process
    """
    logger.info("Update an existing process called {0}".format(args.process_name))
    logger.info("Check that process already exists")
    try:
        process = Process.get_process(args.process_name)
        logger.info("Process {0} already exists and will be updated".format(process.process_name))
    except Process.DoesNotExist:
        logger.info("Process {0} does not already exist in repository".format(args.process_name))
        return 1
    except:
        logger.error("Failed to determine if process already exists in repository", exc_info=args.debug)
        return 1
    if args.processType is not None:
        process.process_type = args.processType
    if args.subjectArea is not None:
        process.subject_area = args.subjectArea
    if args.rowCountType is not None:
        process.row_cnt_type = args.rowCountType
    if args.rowCountProperty is not None:
        process.row_cnt_property = args.rowCountProperty
    if args.currencyDateFlag:
        process.currency_dt_flg = args.currencyDateFlag
    if args.currencyStartOffset is not None:
        process.currency_start_dttm_offset = args.currencyStartOffset
    if args.currencyEndOffset is not None:
        process.currency_end_dttm_offset = args.currencyEndOffset
    process.save()
    logger.info("{0} process successfully updated".format(process.process_name))
    return 0


@LogEntryExit(logger)
def remove_process(args):
    """
    Remove an existing process

    NOTE: Also removes history in process log!!!
    """
    logger.info("Remove process called {0}".format(args.process_name))
    try:
        process = Process.get_process(args.process_name)
        process.remove()
        logger.info("Process {0} successfully removed".format(args.process_name))
    except Process.DoesNotExist:
        logger.error("Process {0} does not exist in repository".format(args.process_name))
        return 1
    except:
        logger.error("Failed to remove process ", exc_info=args.debug)
        return 1
    return 0


@LogEntryExit(logger)
def rename_process(args):
    """
    Rename an existing process name
    """
    logger.info("Rename process called for {0}".format(args.process_name_old))
    try:
        process = Process.get_process(args.process_name_old)
        process.process_name = args.process_name_new
        process.save()
        logger.info("Process {0} successfully renamed".format(args.process_name_new))
    except Process.DoesNotExist:
        logger.error("Process {0} does not exist in repository".format(args.process_name_old))
        return 1
    except:
        logger.error("Failed to rename process ", exc_info=args.debug)
        return 1
    return 0


@LogEntryExit(logger)
def view_process(args):
    """
    View the configuration for a process
    """
    logger.info("Display configuration for process {0}".format(args.process_name))
    try:
        process = Process.get_process(args.process_name)
    except Process.DoesNotExist:
        logger.error("Process {0} does NOT exist in repository".format(args.process_name))
        return 1
    except:
        logger.error("Failed to retrieve process configuration ", exc_info=args.debug)
        return 1
    table_list = Link.get_table_list(args.process_name)
    logger.info("CONFIGURATION FOR PROCESS {0}".format(process.process_name))
    logger.info("{0:<30}: {1}".format('Type',process.process_type))
    logger.info("{0:<30}: {1}".format('Subject Area',process.subject_area))
    logger.info("{0:<30}: {1}".format('Row Count Type',process.row_cnt_type))
    logger.info("{0:<30}: {1}".format('Row Count Property',process.row_cnt_property))
    logger.info("{0:<30}: {1}".format('Currency Date Flag',process.currency_dt_flg))
    logger.info("{0:<30}: {1!s}".format('Currency Start Datetime Offset',process.currency_start_dttm_offset))
    logger.info("{0:<30}: {1!s}".format('Currency End Datetime Offset',process.currency_end_dttm_offset))
    logger.info("{0:<30}: {1}".format('Status',process.status))
    logger.info("{0:<30}: {1}".format('Load Strategy',process.load_strategy))
    logger.info("{0:<30}: {1}".format('Currency Start Datetime',process.currency_start_dttm))
    logger.info("{0:<30}: {1}".format('Currency End Datetime',process.currency_end_dttm))
    logger.info("{0:<30}: {1}".format('Process Start Datetime',process.process_start_dttm))
    logger.info("{0:<30}: {1}".format('Process End Datetime',process.process_end_dttm))
    logger.info("{0:<30}: {1!s}".format('Batch Number',process.batch_nbr))
    logger.info("{0:<30}: {1}".format('Linked Tables',','.join(map(lambda x: "{0[0]}.{0[1]}".format(x),table_list))))
    return 0


@LogEntryExit(logger)
def create_data_currency(args):
    """
    Create an entry for data currency on a table
    """
    logger.info("Create an entry for data currency on the table {0}.{1}".format(args.table_schema,args.table_name))
    logger.info("Check if table is already configured for data currency")
    try:
        currency = DataCurrency.get_currency(args.table_schema, args.table_name)
        logger.warning("Currency for table {0}.{1} already exists".format(currency.table_schema,currency.table_name))
        return 1
    except DataCurrency.DoesNotExist:
        logger.info("Table {0}.{1} not currently configured for data currency"
                    .format(args.table_schema,args.table_name))
    except:
        logger.error("Failed to determine if data currency for table {0}.{1} already exists"
                     .format(args.table_schema,args.table_name), exc_info=args.debug)
        return 1
    currency = DataCurrency.add_currency(args.table_schema,
                                         args.table_name,
                                         args.subjectArea,
                                         args.column_name,
                                         args.currencyDateOffset,
                                         args.currencyQuery)
    logger.info("New data currency entry successfully created for table {0}.{1}"
                .format(currency.table_schema,currency.table_name))
    return 0


@LogEntryExit(logger)
def remove_data_currency(args):
    """
    Remove data currency configuration for a table

    NOTE: Also removes history in data currency log!!
    """
    logger.info("Remove data currency for table {0}.{1}".format(args.table_schema,args.table_name))
    try:
        currency = DataCurrency.get_currency(args.table_schema, args.table_name)
        currency.remove()
        logger.info("Data currency for {0}.{1} and history successfully removed"
                    .format(args.table_schema,args.table_name))
    except DataCurrency.DoesNotExist:
        logger.error("Data Currency for {0}.{1} does not exist in repository".format(args.table_schema,args.table_name))
        return 1
    except:
        logger.error("Failed to remove data currency ", exc_info=args.debug)
        return 1
    return 0


@LogEntryExit(logger)
def rename_data_currency(args):
    """
    Rename data currency configuration for schema or table name
    """
    logger.info("Rename data currency for table {0}.{1}".format(args.table_schema_old,args.table_name_old))
    try:
        currency = DataCurrency.get_currency(args.table_schema_old, args.table_name_old)
    except DataCurrency.DoesNotExist:
        logger.error("Data Currency for {0}.{1} does not exist in repository".format(args.table_schema_old,
                                                                                     args.table_name_old))
        return 1
    except:
        logger.error("Failed to rename data currency", exc_info=args.debug)
        return 1
    currency.table_schema = args.table_schema_new
    currency.table_name = args.table_name_new
    currency.save()
    logger.info("Data currency renamed to {0}.{1}".format(args.table_schema_new,args.table_name_new))
    return 0


@LogEntryExit(logger)
def view_data_currency(args):
    """
    View the data currency configuration for a table
    """
    logger.info("Display configuration for the data currency of table {0}.{1}"
                .format(args.table_schema,args.table_name))
    try:
        currency = DataCurrency.get_currency(args.table_schema,args.table_name)
    except DataCurrency.DoesNotExist:
        logger.info("Table {0}.{1} does NOT exist in repository")
        return 1
    except:
        logger.error("Failed to retrieve data currency configuration", exc_info=args.debug)
        return 1
    logger.info("DATA CURRENCY CONFIGURATION FOR TABLE {0}.{1}".format(currency.table_schema,currency.table_name))
    logger.info("{0:<30}: {1}".format('Currency Column',currency.currency_dttm_column))
    logger.info("{0:<30}: {1}".format('Subject Area',currency.subject_area))
    logger.info("{0:<30}: {1!s}".format('Currency Date Offset Flag',currency.currency_dt_offset_flg))
    logger.info("{0:<30}: {1}".format('Currency Query',currency.currency_dttm_query))
    logger.info("{0:<30}: {1}".format('Active Indicator',currency.active_ind))
    logger.info("{0:<30}: {1!s}".format('Current Currency Date',currency.get_currency_date(currency_date_flag=True)))
    logger.info("{0:<30}: {1!s}"
                .format('Current Currency Date Time',currency.get_currency_date(currency_date_flag=False)))
    return 0


@LogEntryExit(logger)
def link(args):
    """
    Link a process with a data currency entry
    """
    logger.info("Link the process {0} with the data currency for table {1}.{2}"
                .format(args.process_name,args.table_schema,args.table_name))
    logger.info("Check if process is already linked to the data currency of the table")
    table_list = Link.get_table_list(args.process_name)
    if (args.table_schema,args.table_name) in table_list:
        logger.warning("Process {0} already linked to the data currency of table {1}.{2}"
                       .format(args.process_name,args.table_schema,args.table_name))
        return 0
    else:
        try:
            currency = DataCurrency.get_currency(args.table_schema, args.table_name)
            process = Process.get_process(args.process_name)
        except DataCurrency.DoesNotExist:
            logger.error("Data currency entry for table {0}.{1} NOT found".format(args.table_schema, args.table_name),
                         exc_info=args.debug)
            return 1
        except Process.DoesNotExist:
            logger.error("Process {0} NOT found".format(args.process_name), exc_info=args.debug)
            return 1
        except:
            logger.error("Failed to get process or data currency data", exc_info=args.debug)
            return 1
        logger.info("Link the process to the data currency of the table")
        temp_link = Link.add_link(process,currency)
        logger.info("Process {0} now linked with data currency of table {1}.{2}"
                    .format(args.process_name,args.table_schema,args.table_name))
        logger.debug("Link Key= {0!s}".format(temp_link.link_key))
        return 0


@LogEntryExit(logger)
def delink(args):
    """
    De-Link a process with a data currency entry
    """
    logger.info("De-Link the process {0} with the data currency for table {1}.{2}"
                .format(args.process_name,args.table_schema,args.table_name))
    logger.info("Check if process is linked to the data currency of the table")
    table_list = Link.get_table_list(args.process_name)
    if (args.table_schema,args.table_name) in table_list:
        logger.info("Confirmed that process {0} is linked to the data currency of table {1}.{2}"
                    .format(args.process_name,args.table_schema,args.table_name))
        Link.remove_link(args.process_name,args.table_schema,args.table_name)
        logger.info("Process {0} now de-linked with data currency of table {1}.{2}"
                    .format(args.process_name,args.table_schema,args.table_name))
        return 0
    else:
        logger.error("Process {0} NOT linked to table {1}.{2}"
                     .format(args.process_name,args.table_schema,args.table_name), exc_info=args.debug)
        return 1


###############################################################################################################
#
#    ##   ##   ###   #######  ##    #
#    # # # #  #   #     #     # #   #
#    #  #  #  #####     #     #  #  #
#    #     #  #   #     #     #   # #
#    #     #  #   #  #######  #    ##
#
####################################################

def main():
    """
    Main Function
    :return: exit code
    """
    try:
        # Setup argument parser
        parser = argparse.ArgumentParser(description=program_license,
                                         formatter_class=argparse.RawDescriptionHelpFormatter)
        parser.add_argument("-S", "--silent", action="store_true",
                            help="Disable screen output of log messages [default: %(default)s]")
        parser.add_argument("-D", "--debug", action="store_true", help="Debug level logging [default: %(default)s]")
        parser.add_argument("-L", "--log-file", dest="log_file", help="Name and path of log file")
        parser.add_argument('-V', '--version', action='version', version=program_version_message)
        subparsers = parser.add_subparsers(title='Commands',description='List of Commands',
                                           help='Command Description', metavar="")
        if adminuser:
            parser_a = subparsers.add_parser('start', help='Start a new job')
            parser_a.add_argument('-s','--step', type=step_range, default=[-1,-1],
                                  help="Job step or range of steps to run")
            parser_a.add_argument('-j',"--job_file", help="Name and path of the job file in CSV format")
            parser_a.add_argument('-r',"--resume", action="store_true", help="Resume failed, paused or stopped job")
            parser_a.add_argument("--skip", dest="skipEventList", nargs='?', const='all', action='append', default=[],
                                  help="Events to skip [<event_name>|wait|create|all]")
            parser_a.add_argument("--tempargs", type=test_temp_args, nargs='+', dest="temp_args", default={},
                                  help="Temporary arg overrides [key:value key1:value1 ...]")
            parser_a.add_argument("--no-success-email",action="store_true",help="Suppress success emails")
            parser_a.add_argument("job_name", help="Unique name for job or job group")
            parser_a.set_defaults(func=start_job)
            parser_b = subparsers.add_parser('stop', help='Stop a running job after the current step completes')
            parser_b.add_argument("job_name", help="Unique name for job")
            parser_b.set_defaults(func=stop_job)
            parser_c = subparsers.add_parser('pause', help='Pause a running job after the current step completes')
            parser_c.add_argument("job_name", help="Unique name for job")
            parser_c.set_defaults(func=pause_job)

        parser_d = subparsers.add_parser('job', help='Manage jobs')
        subparsers_d = parser_d.add_subparsers(title='Job Commands',description='List of Job Commands',
                                               help='Job Command Description', metavar="")
        parser_db = subparsers_d.add_parser('export', help='Export the job metadata from the repository to a file')
        parser_db.add_argument("job_name", help="Unique name for job")
        parser_db.add_argument("job_file", type=argparse.FileType('wb'),
                               help="Name and path of the job file in CSV format")
        parser_db.set_defaults(func=write_metadata)
        if adminuser:
            parser_da = subparsers_d.add_parser('import', help='Import a job file to the repository')
            parser_da.add_argument('-f','--force', action="store_true",
                                   help="Force replacement of existing job metadata [default: %(default)s]")
            parser_da.add_argument('-g','--group', dest="job_group", default=None,
                                   help="Name of job group [default: %(default)s]")
            parser_da.add_argument("job_name", help="Unique name for job")
            parser_da.add_argument("job_file", help="Name and path of the job file in CSV format")
            parser_da.set_defaults(func=store_metadata)
            parser_dc = subparsers_d.add_parser('rename', help='Rename the job')
            parser_dc.add_argument("job_name_old", help="Existing name for job")
            parser_dc.add_argument("job_name_new", help="New name for job")
            parser_dc.set_defaults(func=rename_job)
            parser_dd = subparsers_d.add_parser('group', help='Change job group')
            parser_dd.add_argument("job_name", help="Name of job")
            parser_dd.add_argument("group_name", help="Name of job group")
            parser_dd.set_defaults(func=change_job_group)
            parser_de = subparsers_d.add_parser('reset', help='Reset job status to not running (USE WITH CAUTION)')
            parser_de.add_argument("job_name", help="Name of job")
            parser_de.set_defaults(func=reset_job_status)
            parser_df = subparsers_d.add_parser('remove', help='Remove a job including job history (USE WITH CAUTION')
            parser_df.add_argument("job_name", help="Name of job")
            parser_df.set_defaults(func=remove_job)

        parser_e = subparsers.add_parser('event', help='Manage events related to jobs')
        subparsers_e = parser_e.add_subparsers(title='Event Commands',description='List of Event Commands',
                                               help='Event Command Description', metavar="")
        if adminuser:
            parser_ea = subparsers_e.add_parser('add', help='Add an event to a job')
            parser_ea.add_argument('-a',"--event_action_type", choices=EVENT_ACTION_TYPE, default=EVENT_ACTION_WAIT,
                                   help="Type of action for event [default: %(default)s]")
            parser_ea.add_argument('-t',"--event_type", choices=EVENT_TYPE, default=EVENT_TYPE_FILE,
                                   help="Type of event [default: %(default)s]")
            parser_ea.add_argument("job_name", help="Unique name for job")
            parser_ea.add_argument("event_name", help="Unique name for event")
            parser_ea.add_argument("event_item", help="Name of file or function associated with event")
            parser_ea.set_defaults(func=add_event)
            parser_eb = subparsers_e.add_parser('disable', help='Disable an event for a job')
            parser_eb.add_argument("job_name", help="Unique name for job")
            parser_eb.add_argument("event_name", nargs='+', help="Unique name(s) for event")
            parser_eb.set_defaults(func=disable_event)
            parser_ec = subparsers_e.add_parser('remove', help='Remove an event for a job')
            parser_ec.add_argument("job_name", help="Unique name for job")
            parser_ec.add_argument("event_name", nargs='+', help="Unique name(s) for event")
            parser_ec.set_defaults(func=remove_event)
            parser_ed = subparsers_e.add_parser('enable', help='Enable an event for a job')
            parser_ed.add_argument("job_name", help="Unique name for job")
            parser_ed.add_argument("event_name", nargs='+', help="Unique name(s) for event")
            parser_ed.set_defaults(func=enable_event)
            parser_ef = subparsers_e.add_parser('rename', help='Rename an event for a job')
            parser_ef.add_argument("job_name", help="Unique name for job")
            parser_ef.add_argument("event_name_old", help="Old name for event")
            parser_ef.add_argument("event_name_new", help="New name for event")
            parser_ef.set_defaults(func=rename_event)
        parser_ee = subparsers_e.add_parser('view', help='View event(s) for a job')
        parser_ee.add_argument("job_name", help="Unique name for job")
        parser_ee.set_defaults(func=view_event)

        parser_g = subparsers.add_parser('process', help='Manage a process')
        subparsers_g = parser_g.add_subparsers(title='Process Commands',
                                               description='List of Process Commands',
                                               help='Process Command Description', metavar="")
        if adminuser:
            parser_ga = subparsers_g.add_parser('add', help='Add a new process')
            parser_ga.add_argument('--process_type', dest="processType",
                                   choices=PROCESS_TYPE, default=PROCESS_TYPE_INGESTION,
                                   help="Type of Process [default: %(default)s]")
            parser_ga.add_argument('--subject_area', dest="subjectArea", default="PSOL",
                                   help="Subject area of Process [default: %(default)s]")
            parser_ga.add_argument('--row_count_type', dest="rowCountType", default="Temp Table",
                                   help="Type of table for row counts [default: %(default)s]")
            parser_ga.add_argument('--row_count_property', dest="rowCountProperty",
                                   help="Name of object used for counting rows")
            parser_ga.add_argument('--currency_date_flag', dest="currencyDateFlag", action="store_true",
                                   help="Use only currency date excluding the time [default: %(default)s]")
            parser_ga.add_argument('--currency_start_offset', dest="currencyStartOffset", type=int, default=0,
                                   help="Offset for the currency start date/time [default: %(default)s]")
            parser_ga.add_argument('--currency_end_offset', dest="currencyEndOffset", type=int, default=0,
                                   help="Offset for the currency end date/time [default: %(default)s]")
            parser_ga.add_argument("process_name", help="Name of new process")
            parser_ga.set_defaults(func=create_process)
            parser_gb = subparsers_g.add_parser('remove', help='Remove a process and its history (USE WITH CAUTION)')
            parser_gb.add_argument("process_name", help="Name of process")
            parser_gb.set_defaults(func=remove_process)
            parser_gd = subparsers_g.add_parser('rename', help='Rename a process name')
            parser_gd.add_argument("process_name_old", help="Name of OLD process")
            parser_gd.add_argument("process_name_new", help="Name of NEW process")
            parser_gd.set_defaults(func=rename_process)
            parser_ge = subparsers_g.add_parser('update', help='Update an existing process')
            parser_ge.add_argument('--process_type', dest="processType",
                                   choices=PROCESS_TYPE, help="Type of Process")
            parser_ge.add_argument('--subject_area', dest="subjectArea",
                                   help="Subject area of Process")
            parser_ge.add_argument('--row_count_type', dest="rowCountType",
                                   help="Type of table for row counts")
            parser_ge.add_argument('--row_count_property', dest="rowCountProperty",
                                   help="Name of object used for counting rows")
            parser_ge.add_argument('--currency_date_flag', dest="currencyDateFlag", action="store_true",
                                   help="Use only currency date excluding the time")
            parser_ge.add_argument('--currency_start_offset', dest="currencyStartOffset", type=int,
                                   help="Offset for the currency start date/time")
            parser_ge.add_argument('--currency_end_offset', dest="currencyEndOffset", type=int,
                                   help="Offset for the currency end date/time")
            parser_ge.add_argument("process_name", help="Name of existing process")
            parser_ge.set_defaults(func=update_process)
        parser_gc = subparsers_g.add_parser('view', help='View process configuration')
        parser_gc.add_argument("process_name", help="Name of process")
        parser_gc.set_defaults(func=view_process)

        parser_h = subparsers.add_parser('currency', help='Configure data currency for a table')
        subparsers_h = parser_h.add_subparsers(title='Data Currency Commands',
                                               description='List of Data Currency Commands',
                                               help='Data Currency Command Description', metavar="")
        if adminuser:
            parser_ha = subparsers_h.add_parser('configure', help='Configure data currency for a table')
            parser_ha.add_argument('--subject_area', dest="subjectArea", default="PSOL",
                                   help="Subject area of Process [default: %(default)s]")
            parser_ha.add_argument('--currency_date_offset', dest="currencyDateOffset",
                                   action="store_true", help="Round currency date down [default: %(default)s]")
            parser_ha.add_argument('--currency_dttm_query', dest="currencyQuery", help="Query override")
            parser_ha.add_argument("table_schema", help="Name of schema")
            parser_ha.add_argument("table_name", help="Name of table")
            parser_ha.add_argument("column_name", help="Name of column with currency date")
            parser_ha.set_defaults(func=create_data_currency)
            parser_hb = subparsers_h.add_parser('remove',
                                                help='Remove currency configuration and the history (USE WITH CAUTION)')
            parser_hb.add_argument("table_schema", help="Name of schema")
            parser_hb.add_argument("table_name", help="Name of table")
            parser_hb.set_defaults(func=remove_data_currency)
            parser_hd = subparsers_h.add_parser('rename', help='Rename schema or table name')
            parser_hd.add_argument("table_schema_old", help="Name of OLD schema")
            parser_hd.add_argument("table_name_old", help="Name of OLD table")
            parser_hd.add_argument("table_schema_new", help="Name of NEW schema")
            parser_hd.add_argument("table_name_new", help="Name of NEW table")
            parser_hd.set_defaults(func=rename_data_currency)
        parser_hc = subparsers_h.add_parser('view', help='View currency configuration for a table')
        parser_hc.add_argument("table_schema", help="Name of schema")
        parser_hc.add_argument("table_name", help="Name of table")
        parser_hc.set_defaults(func=view_data_currency)

        if adminuser:
            parser_i = subparsers.add_parser('link', help='Link a process with the data currency for a table')
            subparsers_i = parser_i.add_subparsers(title='Link Commands',
                                                   description='List of Link Commands',
                                                   help='Link Command Description', metavar="")
            parser_ia = subparsers_i.add_parser('create', help='Create the link')
            parser_ia.add_argument("process_name", help="Name of new process")
            parser_ia.add_argument('table_schema', help="Name of schema")
            parser_ia.add_argument("table_name", help="Name of table")
            parser_ia.set_defaults(func=link)
            parser_ib = subparsers_i.add_parser('remove', help='Remove the link')
            parser_ib.add_argument("process_name", help="Name of new process")
            parser_ib.add_argument('table_schema', help="Name of schema")
            parser_ib.add_argument("table_name", help="Name of table")
            parser_ib.set_defaults(func=delink)

        parser_t = subparsers.add_parser('listall', help='List all jobs in repository')
        parser_t.add_argument('--status',help="Filter on status")
        parser_t.set_defaults(func=list_jobs,job_name=None)
        if adminuser:
            parser_x = subparsers.add_parser('repo', help='Repository Maintenance')
            subparsers_x = parser_x.add_subparsers(title='Repo Commands',
                                                   description='List of Repo Commands',
                                                   help='Repo Command Description', metavar="")
            parser_xa = subparsers_x.add_parser('create', help='Create repository')
            parser_xa.set_defaults(func=create_repository,job_name=None)
            parser_xc = subparsers_x.add_parser('maintenance', help='Enable or disable maintenance mode')
            parser_xc.set_defaults(func=toggle_maintenance_mode,job_name=None)

        # Process arguments

        args = parser.parse_args()

        if args.log_file:
            job_log_file = args.log_file
        else:
            try:
                log_file_identifier = args.job_name
            except AttributeError:
                log_file_identifier = None

            if not log_file_identifier:
                try:
                    log_file_identifier = args.process_name
                except AttributeError:
                    log_file_identifier = None

            if not log_file_identifier:
                try:
                    log_file_identifier = "{0}_{1}".format(args.table_schema,args.table_name)
                except AttributeError:
                    log_file_identifier = None

            # Configure Logging
            try:
                if log_file_identifier:
                    base_log_file = "{0}_{1}_{2}.{3}".format(os.path.join(config.logFile.location,
                                                                          config.logFile.prefix),
                                                             log_file_identifier,
                                                             datetime.now().strftime(config.logFile.dateFormat),
                                                             config.logFile.extension)
                else:
                    base_log_file = "{0}_{1}.{2}".format(os.path.join(config.logFile.location,config.logFile.prefix),
                                                         datetime.now().strftime(config.logFile.dateFormat),
                                                         config.logFile.extension)
            except:
                sys.stderr.write("CRITICAL ERROR: Log file configuration missing from config file\n")
                send_critical_email("Log file configuration missing from config file",args)
                sys.exit(1)
            job_log_file = base_log_file

        fh = GroupFileHandler(os.path.expandvars(job_log_file))
        if args.debug:
            fh.setLevel(logging.DEBUG)
        else:
            fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        # if no screen output is desired, comment out this section
        if not args.silent:
            ch = logging.StreamHandler()
            ch.setFormatter(formatter)
            if args.debug:
                ch.setLevel(logging.DEBUG)
            else:
                ch.setLevel(logging.INFO)
            logger.addHandler(ch)

        #=======================================================================
        # To add email logging support
        # import logging.handlers
        # eh = logging.handlers.SMTPHandler('localhost','jwd3@adcppapi001','jason.decorte@equifax.com','log email test')
        # eh.setFormatter(formatter)
        # eh.setLevel(logging.CRITICAL)
        # logger.addHandler(eh)
        #=======================================================================

        # Start the program
        logger.info("Start {0} ".format(program_name))
        logger.debug("Args: {0!r}".format(args))

        # Check repository version
        if args.func.func_name != 'create_repository':
            try:
                repo = RepoHist.get()
            except ImproperlyConfigured as emsg:
                email_msg = "Failed to get repository version due to improper configuration [{0!s}]".format(emsg)
                logger.error(email_msg, exc_info=args.debug)
                send_critical_email(email_msg,args)
                sys.exit(1)
            except OperationalError as emsg:
                email_msg = "Failed to connect to the repository [{0!s}]".format(emsg)
                logger.error(email_msg, exc_info=args.debug)
                send_critical_email(email_msg,args)
                sys.exit(1)
            else:
                curr_ver = repo.get_version()
                maintenance_mode = repo.is_maintenance_mode()

            if curr_ver != repository_version:
                email_msg = "The repository must be patched. Current version: {0} Required Version: {1!s}".format(
                    curr_ver,repository_version)
                logger.error(email_msg)
                logger.info("End {0}".format(program_name))
                send_critical_email(email_msg,args)
                sys.exit(1)
            if maintenance_mode and args.func.func_name == 'start_job':
                logger.warning("Repository is in maintenance and no new jobs can be started")
                logger.info("End {0}".format(program_name))
                sys.exit(0)

        logger.debug("Calling function {0}".format(args.func.func_name))
        rc = args.func(args)
        logger.info("End {0}".format(program_name))
        sys.exit(rc)
    except KeyboardInterrupt:
        # handle keyboard interrupt
        sys.exit(0)
    except Exception, e:
        # noinspection PyUnboundLocalVariable
        if 'args' in globals().keys() and args.debug:
            import traceback
            traceback.print_exc()
        indent = " " * len(program_name)
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help\n")
        sys.exit(2)


if __name__ == "__main__":
    sys.exit(main())
