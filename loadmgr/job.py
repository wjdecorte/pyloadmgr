"""
Created on Aug 11, 2014

@author: jwd3

Version History
0.2 jwd 09/10/2014
    Added new event class
    Removed default config from genericConfig call 9/30
0.5 jwd 10/16/2014
    NOTE: Versions 0.3 and 0.4 were skipped to bring all modules in line with same version
    Added Event.create method for creating events
    Added Event.reset method for resetting create events (for ex, remove trigger file created from previous run) 10/21
0.6 jwd 12/01/2014
    No Changes
0.7 jwd 01/14/2015
    Removed Job class - Never implemented and now replaced with ORM
    Updated Task class - convert step fields to integer
    Updated Task class - ignore_error and capture_output fields are now passed in as boolean instead of 'y' or 'n'
    Updated Event class - Removed event_active_ind (only active events are loaded)
    Removed Task methods isArgsNull and isCompleted
0.7.1 jwd3 2/17/2017
    Updated package locations

NOTE: Changing the name of this module will cause an issue with the job_queue and job_event fields in job_run table
      since they are pickled using the original name "job".
"""
__version__ = "0.7.1"
__date__ = '2014-08-12'
__updated__ = '02/17/2017'

import os
from time import sleep

from loadmgr.metadata.constants import EVENT_TYPE_OTHER,EVENT_TYPE_FILE
from loadmgr import cfg


class EventError(Exception):
    """
    Define an exception for a generic job error
    """
    def __init__(self, msg=''):
        self._msg = msg
        Exception.__init__(self, msg)

    def __repr__(self):
        return self._msg
    __str__ = __repr__


class Task(object):
    """
    Defines methods and attributes of a task, one step in a job
    """

    def __init__(self, step, description, task_function, command, args, ignore_error,
                 capture_output, wait_step, alt_wait_step):
        """
        Initialize a new instance of the Task class
        """
        self.step = int(step)
        self.description = description
        self.taskFunction = task_function
        self.command = command
        self.args = args
        self.wait_step = int(wait_step)
        self.alt_wait_step = int(alt_wait_step)
        self.ignoreError = ignore_error
        self.captureOutput = capture_output
        self.taskCompleted = False
        self.startTime = None
        self.endTime = None
        self.status = None
        self.taskOutput = None

    def reset_task(self):
        """
        reset the task to not completed
        """
        self.taskCompleted = False
        self.startTime = None
        self.endTime = None
        self.status = None
        self.taskOutput = None

    def store_results(self,start_time,end_time,status,captured_output=None):
        """
        Save the run results of the task
        """
        self.startTime = start_time
        self.endTime = end_time
        self.status = status
        if self.captureOutput:
            self.taskOutput = captured_output

    def set_to_completed(self):
        """
        Set the task to completed
        """
        self.taskCompleted = True

    def get_wait_steps(self):
        """
        Return the wait steps
        """
        return self.wait_step,self.alt_wait_step

    def __str__(self):
        """
        formatted view of task
        """
        task_completion = "Yes" if self.taskCompleted else "No"
        formatted_task = '''Task Name: {0}
        Step: {1}
        Task Type: {2}\tCommand: {3}\tParameter: {4}
        Dependent Step(s): {5}
        Ignore Errors: {6}
        Capture Output: {12}
        Task Completed: {7}
        Start Time: {8}\tFinish Time: {9}
        Status: {10}
        Captured Output: {11!r}\n'''.format(self.description,self.step,self.taskFunction,self.command,self.args,
                                            self.get_wait_steps(),self.ignoreError,task_completion,self.startTime,
                                            self.endTime,self.status,self.taskOutput,self.captureOutput)
        return formatted_task

#===============================================================================
#     def getDescription(self):
#         """ Return the task description """
#         return self.description
# 
#     def getCommand(self):
#         """ Return the task command """
#         return self.command
# 
#     def getTaskFunction(self):
#         """ Return the task type """
#         return self.taskFunction
# 
#     def getStartTime(self):
#         """ Return task start time. """
#         return self.startTime
# 
#     def getEndTime(self):
#         """ Return the task finish time. """
#         return self.endTime
# 
#     def getParameter(self):
#         """ Return the args for the task """
#         return self.args
#===============================================================================


class Event(object):
    """
    Defines methods and attributes of an event
    """

    def __init__(self, event_name, event_action_type, event_type, event_file_name=None, event_function=None):
        """
        Initialize a new instance of the Event class
        """
        self.name = event_name
        self.actionType = event_action_type
        self.type = event_type
        self.filename = event_file_name
        self.function = event_function
        self.complete = False
        self.timeoutReached = False

    def timed_out(self):
        """
        return true if the event timed out before completing else return false
        """
        return self.timeoutReached

    def reset(self):
        """
        Reset the create event
        """
        # Check event type
        if self.type == EVENT_TYPE_FILE:
            if os.path.exists(os.path.expandvars(self.filename)):
                os.remove(os.path.expandvars(self.filename))
            return True
        elif self.type == EVENT_TYPE_OTHER:
            raise EventError("Event Type Other is NOT currently supported!!!!")
        else:
            raise EventError("Invalid Event Type")
        
    def create(self):
        """
        Create the event
        """
        # Check event type
        if self.type == EVENT_TYPE_FILE:
            fname = os.path.expandvars(self.filename)
            with open(fname,'a'):
                os.utime(fname,None)  # similar to Touch in Unix
                self.complete = True
            return True
        elif self.type == EVENT_TYPE_OTHER:
            raise EventError("Event Type Other is NOT currently supported!!!!")
        else:
            raise EventError("Invalid Event Type")
      
    def wait(self):
        """
        Wait for the event to be true
        """
        if self.type == EVENT_TYPE_FILE:
            total_wait_time = 0
            while not self.complete and (cfg.waitTimeOut == 0 or total_wait_time < cfg.waitTimeOut):
                if os.path.exists(os.path.expandvars(self.filename)):
                    os.remove(os.path.expandvars(self.filename))
                    self.complete = True
                else:
                    sleep(cfg.waitSleepTime)
                    total_wait_time += cfg.waitSleepTime
            if 0 < cfg.waitTimeOut <= total_wait_time:
                self.timeoutReached = True
                return False
            return True
        elif self.type == EVENT_TYPE_OTHER:
            raise EventError("Event Type Other is NOT currently supported!!!!")
        else:
            raise EventError("Invalid Event Type")
