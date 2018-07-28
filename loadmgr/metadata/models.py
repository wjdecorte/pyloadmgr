"""
Created on Jan 8, 2015

@author: jwd3

Version History:
0.01 jwd 03/06/2015
    Remove patch code - Separate scripts will be used for patching going forward
    Remove references to currdb - no longer used since merged with process (util) db
    Converted version to string from float
    Added JobEvent.get_event_list method to return a list of events for a specific job
    Added job_group attribute to Job table
    Added Job.get_job_group method
    Added JobRun.add_job_run method for creating new job run entries
    Added JobRunHist, JobRunStepHist, JobRunEventHist objects to the __all__
    Added .capitalize() to job_step_ignore_error and job_step_capture_output in JobStep.load method
0.02 jwd 5/1/2015
    Add timeout parameter to database connection - reduce "database is locked" error
0.03 jwd 6/23/2015
    Added currency_end_dttm_offset to PROCESS and PROCESS_LOG
    Added duration to PROCESS_LOG
    Added duration to JOB_RUN_STEP_HIST
    Added duration to JOB_RUN_HIST
    Added calculate_duration function for populating the new duration fields
    Added new class JSONField
    Added job_temp_args field
    Changed JSONField to return empty dictionary if column is null
    Added .capitalize() method to ignore error and capture output values read from file
0.04 jwd 7/14/2015
    HOTFIX: Added currency_end_dttm_offset to add_process() in the call to create method
0.05 jwd 8/17/2015
    Adding MySQL support
    Fixed weak warning about white space before a comma
    Changed job_run_id from Integer to Big Integer
    Removed DBRequest decorator from JobRun.set_status and JobRun.end since they don't do any database activity
    Changed self.store() to self.save() in JobRun.prepare_run to avoid double use of db request decorator
0.06 jwd 8/27/2015
    Added FileLog table
    Added get_latest_log method to Process
0.07 jwd 10/30/2015
    HOTFIX: Added FileLog to the create_tables function
0.08 jwd3 12/15/2015
    HOTFIX: Added filter to Process.get_last_run_date() to check status of Complete
0.09 jwd3 01/11/2016
    Updated JobStep.load method - now select query is passed in and removed literal_eval function
0.10 jwd3 01/28/2016
    Added repodb, utildb to __all__ for import into patch utility
0.11 jwd3 03/16/2016
    Removed eval and capitalize from loop to load temporary job file instead of repo in JobStep.prepare_run
0.12 jwd3 10/07/2016 HOTFIX
    Added support for Postgres database
    Removed import sys from create_tables function
    Removed second database, utildb
    Added playhouse.db_url.connect to generalize the database connection code
0.13 jwd3 02/16/2017 Maintenance Release
    Change path to password library
    Fixed lines extending beyond 120
0.14 jwd3 04/13/2017 v1.2.4
    Replaced built-in Password package with pswdfile module
0.15 jwd3 05/11/2017 v2.0.0
    Versioning changed to 0.xx where xx is sequential number representing each change to this file
        advanced versioning with major/minor isn't needed
    Added remove method to class Job
    Removed deprecated ProcessBaseModel
0.16 jwd3 05/24/2017 v2.1.2
    Updated JobStep.get_export_query order by clause to use job_step instead of job_step_key
"""
__version__ = "0.16"
__date__ = '2015-01-08'
__updated__ = '05/24/2017'
__all__ = ["Job",
           "JobFile",
           "JobStep",
           "JobEvent",
           "JobRun",
           "JobRunFlag",
           "JobRunHist",
           "JobRunStepHist",
           "JobRunEventHist",
           "create_tables",
           "save_previous_run",
           "fn",
           "RepoHist",
           "Process",
           "ProcessLog",
           "DataCurrency",
           "DataCurrencyLog",
           "Link",
           "FileLog",
           "repodb"]

import base64
import cPickle
import json
import logging
import os
import sys
from datetime import datetime

from loadmgr.utils.decorators import DBRequest
from loadmgr.metadata.constants import EVENT_ACTION_CREATE,CALL_CREATE_EVENT,PROCESS_STATUS_COMPLETE,EVENT_TYPE_OTHER
from loadmgr.metadata.constants import JOB_STATUS_NOT_RUNNING,JOB_STATUS_READY,JOB_STATUS_WAITING,EVENT_TYPE_FILE
from loadmgr.job import Task,Event
from loadmgr import cfg,repository_version
from peewee import *
from playhouse.db_url import connect
from pswdfile.password import Password

db_auth_template = "{user}:{password}@{host}:{port}"
db_url_template = "{dbtype}://{auth}/{dbname}"
dbname = os.path.expandvars(cfg.databases.repodb.name)  # Check for env variable esp for sqlite
dbhost = cfg.databases.repodb.host
dbport = cfg.databases.repodb.port
dbuser = cfg.databases.repodb.user
if cfg.databaseType != 'sqlite':
    password_file = os.path.expandvars(cfg.databases.repodb.passwordFile)
    p = Password(host=dbhost,
                 username=dbuser,
                 data_file_name=os.path.basename(password_file),
                 data_file_dir=os.path.dirname(password_file))
    pswd = p.decrypt()
    if not pswd:
        raise EnvironmentError("CRITICAL ERROR:  Unable to retrieve password for database connection")
    db_auth = db_auth_template.format(user=dbuser, password=pswd, host=dbhost, port=dbport)
else:
    db_auth = ''

db_url = db_url_template.format(dbtype=cfg.databaseType,auth=db_auth,dbname=dbname)
repodb = connect(db_url)


class PickledField(TextField):
    """
    Field definition for a pickled field
    """

    def db_value(self, value):
        """Convert the python value for storage in the database."""
        return value if value is None else base64.b64encode(cPickle.dumps(value))

    def python_value(self, value):
        """Convert the database value to a pythonic value."""
        return value if value is None else cPickle.loads(base64.b64decode(value))


class JSONField(TextField):
    """
    Field definition for storing a JSON string
    """

    def db_value(self, value):
        """Convert the python value for storage in the database."""
        return json.dumps(value) if value else None

    def python_value(self, value):
        """Convert the database value to a pythonic value."""
        return dict() if value is None else json.loads(value)


class BaseModel(Model):
    create_dttm = DateTimeField(default=datetime.now())
    modify_dttm = DateTimeField(default=datetime.now())

    def save(self, *args, **kwargs):
        self.modify_dttm = datetime.now()
        return super(BaseModel, self).save(*args, **kwargs)

    class Meta:
        database = repodb


class Job(BaseModel):

    job_key = PrimaryKeyField()
    job_name = TextField(null=True)
    job_file = TextField(null=True)
    job_group = TextField(null=True)

    class Meta:
        db_table = 'job'

    '''
    Convenience methods for model
    '''
    @classmethod
    @DBRequest(repodb)
    def get_job(cls,job_name):
        """
        Get job data from repository
        """
        return cls.get(cls.job_name == job_name)

    @classmethod
    @DBRequest(repodb)
    def get_job_group(cls,job_group):
        """
        Get job data for all jobs in the job group from repository
        """
        return [x for x in cls.select().where(cls.job_group == job_group)]

    @classmethod
    @DBRequest(repodb)
    def add_job(cls, job_name, job_file_name, job_group):
        """
        Add a job to the repository
        """
        return cls.create(job_name=job_name,
                          job_file=job_file_name,
                          job_group=job_group)

    @classmethod
    @DBRequest(repodb)
    def get_job_list_with_info(cls):
        """
        Get a list of all the jobs and their related info in the repository
        """
        return [x for x in cls.select(Job,JobRun).join(JobRun, JOIN_LEFT_OUTER).order_by(Job.job_name)]

    @DBRequest(repodb)
    def remove(self):
        """
        remove job, job steps and job history from repository
        """
        self.delete_instance(recursive=True)


class JobStep(BaseModel):
    job_step_key = PrimaryKeyField()
    job_key = ForeignKeyField(db_column='job_key', rel_model=Job, to_field='job_key', related_name='steps')
    job_step = IntegerField(null=True)
    job_step_description = TextField(null=True)
    job_step_function = TextField(null=True)
    job_step_command = TextField(null=True)
    job_step_args = TextField(null=True)
    job_step_ignore_error = BooleanField(null=True)
    job_step_capture_output = BooleanField(null=True)
    job_step_wait_step = IntegerField(null=True)
    job_step_alt_wait_step = IntegerField(null=True)

    class Meta:
        db_table = 'job_step'

    '''
    Convenience methods for model
    '''
    @classmethod
    @DBRequest(repodb)
    def load(cls,job,job_file):
        """
        Load the job steps from the file to the table
        :param job: Model Class
        :param job_file: Job File Model Class Select Query
        :return: None
        """
        for row in job_file:
            cls.create(job_key=job,
                       job_step=row.job_step,
                       job_step_description=row.job_step_description,
                       job_step_function=row.job_step_function,
                       job_step_command=row.job_step_command,
                       job_step_args=row.job_step_args,
                       job_step_ignore_error=row.job_step_ignore_error,
                       job_step_capture_output=row.job_step_capture_output,
                       job_step_wait_step=row.job_step_wait_step,
                       job_step_alt_wait_step=row.job_step_alt_wait_step)
    
    @staticmethod
    @DBRequest(repodb)
    @repodb.atomic()
    def remove(job):
        """
        Delete all the steps associated with a job
        """
        total_rows = 0
        for s in job.steps:
            rc = s.delete_instance()
            total_rows += rc
        return total_rows

    @staticmethod
    @DBRequest(repodb)
    def get_export_query(job):
        """
        Get a query that can be used to export the job steps to a file
        """
        return job.steps.select(JobStep.job_step,
                                JobStep.job_step_description,
                                JobStep.job_step_function,
                                JobStep.job_step_command,
                                JobStep.job_step_args,
                                JobStep.job_step_ignore_error,
                                JobStep.job_step_capture_output,
                                JobStep.job_step_wait_step,
                                JobStep.job_step_alt_wait_step).order_by(JobStep.job_step)
        

class JobEvent(BaseModel):
    job_event_key = PrimaryKeyField()
    job_key = ForeignKeyField(db_column='job_key', rel_model=Job, to_field='job_key', related_name='events')
    event_name = TextField(null=True)
    event_action_type = TextField(null=True)
    event_type = TextField(null=True)
    event_file_name = TextField(null=True)
    event_function = TextField(null=True)
    event_active_ind = BooleanField(null=True)

    class Meta:
        db_table = 'job_event'

    '''
    Convenience methods for model
    '''
    @classmethod
    @DBRequest(repodb)
    def add(cls,job,event_name,event_action_type,event_type,event_item):
        """
        Add event to a job
        """
        event_file_name = event_item if event_type == EVENT_TYPE_FILE else ''
        event_function = event_item if event_type == EVENT_TYPE_OTHER else ''
        cls.create(job_key=job,
                   event_name=event_name,
                   event_action_type=event_action_type,
                   event_type=event_type,
                   event_file_name=event_file_name,
                   event_function=event_function,
                   event_active_ind=True)

    @staticmethod
    @DBRequest(repodb)
    def set_active_ind(job,event_name_list,active_ind):
        """
        Update event(s) active indicator for a job
        """
        for ev in job.events.filter(JobEvent.event_name << event_name_list):
            ev.event_active_ind = active_ind
            ev.save()
            #logger.info("Update event {0} for job {1} to {2}".format(ev.event_name,job.job_name,str(active_ind)))
        
    @staticmethod
    @DBRequest(repodb)
    @repodb.atomic()
    def remove(job,event_name_list):
        """
        Delete event(s) for a job
        """
        for ev in job.events.filter(JobEvent.event_name << event_name_list):
            ev.delete_instance()

    @staticmethod
    @DBRequest(repodb)
    def get_event_list(job):
        """
        Return a list of events with info on each event for that job
        """
        query = job.events.select(JobEvent.event_name,JobEvent.event_action_type,JobEvent.event_type,
                                  JobEvent.event_file_name,JobEvent.event_function,JobEvent.event_active_ind)
        return [x for x in query]


class JobRun(BaseModel):
    job_run_key = PrimaryKeyField()
    job_key = ForeignKeyField(db_column='job_key', rel_model=Job, to_field='job_key', related_name='run')
    job_run_id = BigIntegerField(null=True)
    job_start_dttm = DateTimeField(null=True)
    job_end_dttm = DateTimeField(null=True)
    job_status = TextField(null=True)
    job_pid = IntegerField(null=True)
    job_log_file = TextField(null=True)
    job_start_step = IntegerField(null=True)
    job_end_step = IntegerField(null=True)
    job_queue = PickledField(null=True)
    job_last_completed_step = IntegerField(null=True)
    job_event = PickledField(null=True)
    job_temp_args = JSONField(null=True)

    class Meta:
        db_table = 'job_run'

    '''
    Convenience methods for model
    '''
    @classmethod
    @DBRequest(repodb)
    def get_job_run(cls,job):
        """
        Return the job run data for a job
        """
        return job.run.get()

    @classmethod
    @DBRequest(repodb)
    def add_job_run(cls,job):
        """
        Add a new job run
        """
        return cls.create(job_key=job,job_status=JOB_STATUS_NOT_RUNNING)

    @classmethod
    @DBRequest(repodb)
    def get_job_run_list(cls):
        """
        Get a list of job runs
        """
        return [x for x in cls.select()]

    @DBRequest(repodb)
    def get_job_event_list(self,action_type=None):
        """
        Get a list of job events
        """
        if action_type:
            return [x for x in self.job_event if x.actionType == action_type]
        else:
            return [x for x in self.job_event]

    @DBRequest(repodb)
    def set_value(self,attribute,value):
        """
        Set the value of an attribute and save to the repository
        """
        if hasattr(self,attribute):
            setattr(self,attribute,unicode(value))
            self.save(only=[getattr(JobRun,attribute)])

    def set_status(self,value):
        """
        Special version of setValue method for job status attribute
        """
        self.set_value('job_status',value)

    @DBRequest(repodb)
    def save_queue(self,step):
        """
        Persist queue and last completed step
        """
        self.job_last_completed_step = step
        self.save(only=[JobRun.job_queue,JobRun.job_last_completed_step])

    @DBRequest(repodb)
    def store(self):
        """
        Persist job run data
        """
        self.save()

    @DBRequest(repodb)
    def prepare_run(self,task_query,event_query,job_step_range,event_skip_list,temp_args):
        """
        Prepare for the job run instance for a new execution
        """
        start_step = job_step_range[0]
        end_step = job_step_range[1]
        has_create_events = False
        has_call_create_event = False
        event_list = []
        if event_query.count() > 0:
            # if no event skip list (aka nothing to skip)
            # OR if event skip list is populated and contains NO value of 'all',
            # then process event query .  if found, skip all events
            if (not event_skip_list) or (not (filter(lambda x: x.lower() == 'all',event_skip_list))):
                for row in event_query:
                    # if event skip list is empty OR if not and action type is
                    # not in skip list and event name is not in skip list
                    if not event_skip_list or (row.event_action_type not in event_skip_list
                                               and row.event_name not in event_skip_list):
                        event_list.append(Event(event_name=row.event_name,
                                                event_action_type=row.event_action_type,
                                                event_type=row.event_type,
                                                event_file_name=row.event_file_name,
                                                event_function=row.event_function))
                        if not has_create_events:  # if no create events found yet, check current row
                            has_create_events = row.event_action_type == EVENT_ACTION_CREATE
        self.job_event = event_list
        # Create job queue
        task_dict = {}
        for row in task_query:
            task_key = int(row.job_step)
            job_step_ignore_error = row.job_step_ignore_error
            job_step_capture_output = row.job_step_capture_output
            # Check if the job step (aka task key) is between start step and end step
            # NOTE: Start step or end step value of -1 indicates min or max, respectively.
            if (start_step < 0 or (0 <= start_step <= task_key)) and (end_step < 0
                                                                      or (end_step >= 0 and task_key <= end_step)):
                task_dict.update({task_key:Task(step=row.job_step,
                                                description=row.job_step_description,
                                                task_function=row.job_step_function,
                                                command=row.job_step_command,
                                                args=row.job_step_args,
                                                ignore_error=job_step_ignore_error,
                                                capture_output=job_step_capture_output,
                                                wait_step=row.job_step_wait_step,
                                                alt_wait_step=row.job_step_alt_wait_step)})
            if row.job_step_function == CALL_CREATE_EVENT:  # check if task_function is the call to create events
                has_call_create_event = True
        if has_create_events and not has_call_create_event:
            # logger.warning("Job has create type events AND no call to create events")
            next_step = max(task_dict) + 10
            # logger.debug("Next Step= {0!s}".format(next_step))
            # logger.info("Add function callCreateEvent as last step on queue")
            # logger.debug("add new task object to task list for persistence")
            new_task_obj = Task(step=next_step, description='Create Events', task_function=CALL_CREATE_EVENT,
                                command=None, args=None, ignore_error=True,capture_output=False, wait_step=0,
                                alt_wait_step=0)
            task_dict.update({next_step:new_task_obj})
        self.job_queue = task_dict
        self.job_start_step = min(task_dict)
        self.job_end_step = max(task_dict)
        self.job_status = JOB_STATUS_READY
        self.job_run_id = None
        self.job_start_dttm = None
        self.job_end_dttm = None
        self.job_log_file = None
        self.job_last_completed_step = None
        self.job_temp_args = temp_args
        self.save()
        # Create job run flag record
        JobRunFlag.create(job_run_key=self,
                          pause_flag=False,
                          stop_flag=False)
        
    @DBRequest(repodb)
    def start(self,job_log_file):
        """
        Set the start values of the job run (log file and start time and status of Waiting)
        """
        self.job_log_file = job_log_file
        self.job_start_dttm = datetime.now()
        self.job_run_id = int(str(self.job_key.job_key) + self.job_start_dttm.strftime('%y%m%d%H%M%S'))
        self.job_status = JOB_STATUS_WAITING
        self.save(only=[JobRun.job_log_file,JobRun.job_start_dttm,JobRun.job_run_id,JobRun.job_status])

    def end(self):
        """
        Set the end values of the job run (end time)
        """
        self.job_end_dttm = datetime.now()
        self.store()

    #===========================================================================
    # @staticmethod
    # @dbRequest(repodb)
    # def getStatus(id_value):
    #     '''
    #     Get status of job run
    #     '''
    #     try:
    #         return JobRun.select(JobRun.job_status).where(JobRun.job_run_id == id_value).get().job_status
    #     except:
    #         return None
    #===========================================================================


class JobRunFlag(BaseModel):
    job_run_flag_key = PrimaryKeyField()
    job_run_key = ForeignKeyField(db_column='job_run_key', rel_model=JobRun,
                                  to_field='job_run_key', related_name='flag')
    pause_flag = BooleanField(null=True)
    stop_flag = BooleanField(null=True)

    class Meta:
        db_table = 'job_run_flag'

    '''
    Convenience methods for model
    '''
    @staticmethod
    @DBRequest(repodb)
    def get_flag(job_run):
        """
        Get job run flag
        """
        return job_run.flag.get()
    
    @staticmethod
    @DBRequest(repodb)
    def pause(job_run):
        """
        Set pause flag for a job run
        """
        flag = job_run.flag.get()
        flag.pause_flag = True
        flag.save()
    
    @staticmethod
    @DBRequest(repodb)
    def unpause(job_run):
        """
        Un-set pause flag for a job run
        """
        flag = job_run.flag.get()
        flag.pause_flag = False
        flag.save()
    
    @staticmethod
    @DBRequest(repodb)
    def stop(job_run):
        """
        Set stop flag for a job run
        """
        flag = job_run.flag.get()
        flag.stop_flag = True
        flag.save()


class JobRunHist(BaseModel):
    job_run_hist_key = PrimaryKeyField()
    job_key = ForeignKeyField(db_column='job_key', rel_model=Job, to_field='job_key', related_name='hist_runs')
    job_run_id = BigIntegerField(null=True)
    job_name = TextField(null=True)
    job_start_dttm = DateTimeField(null=True)
    job_end_dttm = DateTimeField(null=True)
    job_status = TextField(null=True)
    job_pid = IntegerField(null=True)
    job_log_file = TextField(null=True)
    job_start_step = IntegerField(null=True)
    job_end_step = IntegerField(null=True)
    job_last_completed_step = IntegerField(null=True)
    job_duration = IntegerField(null=True, default=-1)
    job_temp_args = JSONField(null=True)

    class Meta:
        db_table = 'job_run_hist'


class JobRunEventHist(BaseModel):
    job_run_event_hist_key = PrimaryKeyField()
    job_run_hist_key = ForeignKeyField(db_column='job_run_hist_key', rel_model=JobRunHist,
                                       to_field='job_run_hist_key', related_name='hist_events')
    job_name = TextField(null=True)
    event_name = TextField(null=True)
    event_action_type = TextField(null=True)
    event_type = TextField(null=True)
    event_file_name = TextField(null=True)
    event_function = TextField(null=True)
    event_completed_flg = BooleanField(null=True)
    event_timed_out_flg = BooleanField(null=True)

    class Meta:
        db_table = 'job_run_event_hist'


class JobRunStepHist(BaseModel):
    job_run_step_hist_key = PrimaryKeyField()
    job_run_hist_key = ForeignKeyField(db_column='job_run_hist_key', rel_model=JobRunHist,
                                       to_field='job_run_hist_key', related_name='hist_steps')
    job_name = TextField(null=True)
    job_step = IntegerField(null=True)
    step_description = TextField(null=True)
    step_function = TextField(null=True)
    step_command = TextField(null=True)
    step_args = TextField(null=True)
    step_ignore_error_flg = BooleanField(null=True)
    step_capture_output_flg = BooleanField(null=True)
    step_wait_step = IntegerField(null=True)
    step_alt_wait_step = IntegerField(null=True)
    step_completed_flg = BooleanField(null=True)
    step_start_time = DateTimeField(null=True)
    step_end_time = DateTimeField(null=True)
    step_status = TextField(null=True)
    step_output = TextField(null=True)
    step_duration = IntegerField(null=True, default=-1)

    class Meta:
        db_table = 'job_run_step_hist'


class RepoHist(BaseModel):
    repo_version = TextField(null=True)
    patch_history = TextField(null=True)
    maintenance_mode_flg = BooleanField(null=True)

    class Meta:
        db_table = 'repo_hist'
    '''
    Convenience methods for model
    '''
    @DBRequest(repodb)
    def get_version(self):
        """
        Get the version of the repository
        """
        return self.repo_version

    @DBRequest(repodb)
    def is_maintenance_mode(self):
        """
        Returns True if the repository is in maintenance mode or False if not.
        """
        return self.maintenance_mode_flg


class JobFile(Model):
    job_step = TextField()
    job_step_description = TextField(null=True)
    job_step_function = TextField()
    job_step_command = TextField(null=True)
    job_step_args = TextField(null=True)
    job_step_ignore_error = TextField()
    job_step_capture_output = TextField()
    job_step_wait_step = TextField(null=True,default=0)
    job_step_alt_wait_step = TextField(null=True,default=0)

    class Meta:
        database = SqliteDatabase(':memory:')


class Process(BaseModel):
    process_key = PrimaryKeyField()
    process_name = TextField(null=True)
    process_type = TextField(null=True)
    subject_area = TextField(null=True)
    row_cnt_type = TextField(null=True)
    row_cnt_property = TextField(null=True)
    currency_dt_flg = BooleanField(null=True, default=False)
    currency_start_dttm_offset = IntegerField(null=True, default=0)
    currency_start_dttm = DateTimeField(null=True)
    currency_end_dttm = DateTimeField(null=True)
    status = TextField(null=True)
    process_start_dttm = DateTimeField(null=True)
    process_end_dttm = DateTimeField(null=True)
    batch_nbr = CharField(max_length=50,null=True)
    load_strategy = TextField(null=True)
    currency_end_dttm_offset = IntegerField(null=True, default=0)

    class Meta:
        db_table = 'process'
    '''
    Convenience methods for model
    '''
    @classmethod
    @DBRequest(repodb)
    def get_process(cls, process_name):
        """
        Get process data from repository
        """
        return cls.get(cls.process_name == process_name)

    @classmethod
    @DBRequest(repodb)
    def add_process(cls, process_name, process_type, subject_area, row_count_type,
                    row_count_property, currency_date_flag, currency_start_offset, currency_end_offset):
        """
        Add a process to the repository
        """
        return cls.create(process_name=process_name,
                          process_type=process_type,
                          subject_area=subject_area,
                          row_cnt_type=row_count_type,
                          row_cnt_property=row_count_property,
                          currency_dt_flg=currency_date_flag,
                          currency_start_dttm_offset=currency_start_offset,
                          currency_end_dttm_offset=currency_end_offset)

    @DBRequest(repodb)
    def remove(self):
        """
        remove process and history in process log from repository
        """
        self.delete_instance(recursive=True)

    @DBRequest(repodb)
    def get_last_run_date(self):
        """
        Get the last run date which is the max currency end datetime in the process log for status Complete.
        """
        return self.log.filter(ProcessLog.status ==
                               PROCESS_STATUS_COMPLETE).aggregate(fn.Max(ProcessLog.currency_end_dttm))

    @DBRequest(repodb)
    def get_attribute(self,attribute_name):
        """
        Get an attribute defined for a process.
        """
        return getattr(self,attribute_name)

    @DBRequest(repodb)
    def get_latest_log(self):
        """
        Get the latest process log record for a given process
        :return : ProcessLog Instance
        """
        return ProcessLog.get(process_log_key=self.log.aggregate(fn.Max(ProcessLog.process_log_key)))


class ProcessLog(BaseModel):
    process_log_key = PrimaryKeyField()
    process_key = ForeignKeyField(db_column='process_key', rel_model=Process,
                                  to_field='process_key', related_name='log')
    process_name = TextField(null=True)
    action = TextField(null=True)
    status = TextField(null=True)
    currency_start_dttm = DateTimeField(null=True)
    currency_end_dttm = DateTimeField(null=True)
    nbr_rows = IntegerField(null=True)
    process_start_dttm = DateTimeField(null=True)
    process_end_dttm = DateTimeField(null=True)
    batch_nbr = CharField(max_length=50,null=True)
    load_strategy = TextField(null=True)
    currency_start_dttm_offset = IntegerField(null=True)
    currency_end_dttm_offset = IntegerField(null=True, default=0)
    process_duration = IntegerField(null=True, default=-1)

    class Meta:
        db_table = 'process_log'
    '''
    Convenience methods for model
    '''
    @classmethod
    @DBRequest(repodb)
    def log_process_info(cls, process, action, row_count):
        """
        Log the process info to the repository
        """
        return cls.create(process_key=process,
                          process_name=process.process_name,
                          action=action,
                          status=process.status,
                          currency_start_dttm=process.currency_start_dttm,
                          currency_end_dttm=process.currency_end_dttm,
                          nbr_rows=row_count,
                          process_start_dttm=process.process_start_dttm,
                          process_end_dttm=process.process_end_dttm,
                          batch_nbr=process.batch_nbr,
                          load_strategy=process.load_strategy,
                          currency_start_dttm_offset=process.currency_start_dttm_offset,
                          currency_end_dttm_offset=process.currency_end_dttm_offset,
                          process_duration=calculate_duration(process.process_start_dttm,process.process_end_dttm))


class DataCurrency(BaseModel):
    data_currency_key = PrimaryKeyField()
    table_schema = TextField(null=True)
    table_name = TextField(null=True)
    subject_area = TextField(null=True)
    currency_dttm_column = TextField(null=True)
    currency_dttm_query = TextField(null=True)
    currency_dt_offset_flg = BooleanField(null=True)
    active_ind = BooleanField(null=True,default=True)

    class Meta:
        db_table = 'data_currency'

    '''
    Convenience methods for model
    '''
    @classmethod
    @DBRequest(repodb)
    def get_currency(cls, table_schema, table_name):
        """
        Get data currency data from repository
        """
        return cls.get((cls.table_schema == table_schema) & (cls.table_name == table_name))

    @classmethod
    @DBRequest(repodb)
    def add_currency(cls, table_schema, table_name, subject_area, column_name, currency_date_offset, currency_query):
        """
        Add data currency entry to the repository
        """
        return cls.create(table_schema=table_schema,
                          table_name=table_name,
                          subject_area=subject_area,
                          currency_dttm_column=column_name,
                          currency_dt_offset_flg=currency_date_offset,
                          currency_dttm_query=currency_query)

    @DBRequest(repodb)
    def remove(self):
        """
        remove data currency entry and history in data currency log from repository
        """
        self.delete_instance(recursive=True)

    @DBRequest(repodb)
    def get_currency_date(self, currency_date_flag):
        """
        Get the currency date for the table.

        Return date if currency date flag else return datetime
        """
        date_field = DataCurrencyLog.currency_dt if currency_date_flag else DataCurrencyLog.currency_dttm
        return self.log.aggregate(fn.Max(date_field))


class DataCurrencyLog(BaseModel):
    data_currency_log_key = PrimaryKeyField()
    data_currency_key = ForeignKeyField(db_column='data_currency_key', rel_model=DataCurrency,
                                        to_field='data_currency_key', related_name='log')
    table_name = TextField(null=True)
    currency_dttm = DateTimeField(null=True)
    currency_dt = DateField(null=True)

    class Meta:
        db_table = 'data_currency_log'

    '''
    Convenience methods for model
    '''
    @classmethod
    @DBRequest(repodb)
    def log_data_currency(cls, data_currency, currency_datetime, currency_date):
        """
        Log the data currency for a table
        """
        return cls.create(data_currency_key=data_currency,
                          table_name=data_currency.table_name,
                          currency_dttm=currency_datetime,
                          currency_dt=currency_date)


class Link(BaseModel):
    link_key = PrimaryKeyField()
    process_key = ForeignKeyField(db_column='process_key', rel_model=Process,
                                  to_field='process_key', related_name='links')
    data_currency_key = ForeignKeyField(db_column='data_currency_key', rel_model=DataCurrency,
                                        to_field='data_currency_key', related_name='links')

    class Meta:
        db_table = 'lnk_process_data_currency'

    '''
    Convenience methods for model
    '''
    @classmethod
    @DBRequest(repodb)
    def add_link(cls, process, currency):
        """
        Link a process to a table
        """
        return cls.create(process_key=process,
                          data_currency_key=currency)

    @staticmethod
    @DBRequest(repodb)
    def remove_link(process_name,table_schema,table_name):
        """
        Remove the link between a process and a table
        """
        query = (Link
                 .select(Link, Process, DataCurrency)
                 .join(DataCurrency)
                 .switch(Link)
                 .join(Process)
                 .where((Process.process_name == process_name)
                        & (DataCurrency.table_schema == table_schema)
                        & (DataCurrency.table_name == table_name)))
        link = query.get()
        link.delete_instance()

    @staticmethod
    @DBRequest(repodb)
    def get_table_list(process_name):
        """
        Get list of tables linked to a process
        """
        query = DataCurrency.select().join(Link).join(Process).where(Process.process_name == process_name)
        return [(x.table_schema,x.table_name) for x in query]


class FileLog(BaseModel):
    file_key = PrimaryKeyField()
    process_key = ForeignKeyField(db_column='process_key', rel_model=Process,
                                  to_field='process_key', related_name='file')
    process_log_key = ForeignKeyField(db_column='process_log_key', rel_model=ProcessLog,
                                      to_field='process_log_key', related_name='file')
    file_name = CharField(max_length=200,null=True)
    record_count = IntegerField(null=True)
    file_type = CharField(null=True,max_length=20)
    file_size = FloatField(null=True)
    file_date = DateTimeField(null=True)

    class Meta:
        db_table = 'file_log'


#####################################################################################################################
#                                                                                                                   #
# FUNCTION DEFINITIONS - Utility Functions requiring database manipulation that are not specific to a class         #
#                                                                                                                   #
#####################################################################################################################

@DBRequest(repodb)
@repodb.atomic()
def save_previous_run(job_run):
    """
    Save previous job run into the history tables
    """
    import sys
    logger = logging.getLogger(os.path.basename(sys.argv[0]).split(".")[0] + ".models" + ".savePreviousRun")
    try:
        logger.debug("Remove the run flag record")
        try:
            flag = JobRunFlag.get_flag(job_run)
            flag.delete_instance()
        except JobRunFlag.DoesNotExist:
            logger.debug("Job run flag record doesn't exist")
            pass
        logger.debug("Copy the job run data to the run history table")
        run_hist = JobRunHist.create(job_key=job_run.job_key,
                                     job_run_id=job_run.job_run_id,
                                     job_name=job_run.job_key.job_name,
                                     job_start_dttm=job_run.job_start_dttm,
                                     job_end_dttm=job_run.job_end_dttm,
                                     job_status=job_run.job_status,
                                     job_pid=job_run.job_pid,
                                     job_log_file=job_run.job_log_file,
                                     job_start_step=job_run.job_start_step,
                                     job_end_step=job_run.job_end_step,
                                     job_last_completed_step=job_run.job_last_completed_step,
                                     job_duration=calculate_duration(job_run.job_start_dttm,job_run.job_end_dttm),
                                     job_temp_args=job_run.job_temp_args)
        logger.debug("Get all the steps from the job and copy to the job run step history table")
        for key in sorted(job_run.job_queue.keys()):
            JobRunStepHist.create(job_run_hist_key=run_hist,
                                  job_name=job_run.job_key.job_name,
                                  job_step=job_run.job_queue[key].step,
                                  step_description=job_run.job_queue[key].description,
                                  step_function=job_run.job_queue[key].taskFunction,
                                  step_command=job_run.job_queue[key].command,
                                  step_args=job_run.job_queue[key].args,
                                  step_ignore_error_flg=job_run.job_queue[key].ignoreError,
                                  step_capture_output_flg=job_run.job_queue[key].captureOutput,
                                  step_wait_step=job_run.job_queue[key].wait_step,
                                  step_alt_wait_step=job_run.job_queue[key].alt_wait_step,
                                  step_completed_flg=job_run.job_queue[key].taskCompleted,
                                  step_start_time=job_run.job_queue[key].startTime,
                                  step_end_time=job_run.job_queue[key].endTime,
                                  step_status=job_run.job_queue[key].status,
                                  step_output=job_run.job_queue[key].taskOutput,
                                  step_duration=calculate_duration(job_run.job_queue[key].startTime,
                                                                   job_run.job_queue[key].endTime))
        logger.debug("Get all the events from the job and copy to the job run event history table")
        for event in job_run.job_event:
            JobRunEventHist.create(job_run_hist_key=run_hist,
                                   job_name=job_run.job_key.job_name,
                                   event_name=event.name,
                                   event_action_type=event.actionType,
                                   event_type=event.type,
                                   event_file_name=event.filename,
                                   event_function=event.function,
                                   event_completed_flg=event.complete,
                                   event_timed_out_flg=event.timeoutReached)
        return True
    except:
        logger.error("Failed to save previous job run to history tables", exc_info=True)
        return False


def create_tables():
    """
    Create the repository
    """
    logger = logging.getLogger(os.path.basename(sys.argv[0]).split(".")[0] + ".models" + ".createTables")
    #logger = logging.getLogger('test' + ".models" + ".createTables")
    logger.info("Create Repository DB")
    try:
        repodb.connect()
        repodb.create_tables([Job, JobStep, JobEvent, JobRun, JobRunFlag, JobRunHist, JobRunEventHist,
                              JobRunStepHist, RepoHist], safe=False)
        RepoHist.create(repo_version=repository_version, patch_history='', maintenance_mode_flg=False)
        repodb.close()
        repodb_create = True
    except:
        #repodb.close()
        logger.error("Failed to create the repository")
        logger.warn("Repository may already exist; if so, execute patch script or remove existing repository")
        logger.debug("Traceback", exc_info=True)
        return False
    logger.info("Create Util DB")
    try:
        repodb.connect()
        repodb.create_tables([Process,ProcessLog,DataCurrency,DataCurrencyLog,Link,FileLog], safe=False)
        repodb.close()
        utildb_create = True
    except:
        logger.error("Failed to create the repository")
        logger.warn("Repository may already exist; if so, execute patch script or remove existing repository")
        logger.debug("Traceback", exc_info=True)
        return False
    return repodb_create and utildb_create


def calculate_duration(start,end):
    """
    Calculate the duration in seconds between two dates with time
    :param start: Start date and time
    :param end:  End date and time
    :return: Duration in Seconds
    """
    diff = abs(start - end) if start and end else None
    return diff.seconds + diff.days * 24 * 3600 if diff else 0
