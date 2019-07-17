Purpose
The Load Manager is a generic load application used on a Linux platform for automating a set of steps.  A job is a container used to define those set of steps.  Within a job one or more processes can be added to track groups of related steps.  Processes can then be linked to the data currency of one or more tables to better control which data is loaded or extracted.  To manage all the metadata for the jobs, the processes and the data currency of tables, the Load Manager uses a repository to store all job related metadata in a database.  It currently only supports Sqlite3 databases, but can be easily modified to use MySQL or Postgresql databases.  To automate a job, the metadata describing each step must be imported into the application's repository.  Jobs can also have events that cause the job to wait or events that trigger an action.  Events must be associated to a job prior to execution.  Events can be added, disabled, enabled and removed before the job execution.  After a job starts execution the events can't be altered for the current run.  Processes are a way of grouping like steps together within a single job.  Processes are marked with a start process step and a stop process step.  The process defines the load type, the load strategy, the currency start and end dates, row count method and more.  To assist the process in determining end dates, the data currency for a table can also be configured by the Load Manager.  The configuration consists of the table schema, table name, date column name and date offset.  By adding the log data currency step to the job, the Load Manager will create a new entry in the repository with the maximum date value of the column configured for the data currency for that table.  In order for a process to make use of the data currency values for a table, the process and table need to be linked.  The Load Manager can add this link to the repository.

Details
    Version: 1.2.1
    Release Date: 02/22/2017
    Package: pyloadmgr-1.2.1.tar.gz
    Config: $LOADMGR_CONFIG/loadmgr_config.cfg
    Language: Python (compatible with Python 2.7 only - not tested with Python 3.x)

Release Notes
    v1.2.1
        * Bug fix - Exception encountered when task had no command output
        * Added warning message if job resumed and no steps were left to run

    v1.2
        * Converted to Python distribution
        * Re-organized code
        * Added ability to escape characters in job file
        * Added filter to data currency query to reduce the data set

    v1.1.1
        * Added shell wrapper to configure the environment for load manager application
        * Removed the dependency on the env.cfg file
        * Created new, separate location for the Load Manager application (self-containment)
        * Added job reset command to reset status of a job stuck in the Running status after a system crash
        * Updated the job file CSV import function to allow comments to be added
        * Added three new Job Step Functions: hdfs_file_stage, file_record_count, create_done_file

    v1.0.0
        * Split commands between read-only commands for everyone and admin commands for execution user
        * Added new load strategy Chunk for loading large data volumes in chunks
        * Added new process attribute currency_end_dttm_offset, used with the Chunk load strategy to determine the size of the chunk
        * Added job duration, step duration and process duration
        * Added rename command for process, currency and job
        * Added change job group function for updating the job group
        * Added ability to override step parameters from command line (temporary args)
        * Updated notifications to handle failure, warning and success emails

    v0.8.1
        * Job Group attribute - allows like-jobs to be grouped together for starting, stopping and pausing.
        * View all events associated with a job (loadmgr.py event view <job_name>).
        * Using the process add command on an existing process will update the metadata for that process.
        * When starting a job in silent mode, an email will be sent if the job is already running.
        * The job import command updates the job attributes now as well as the job steps for existing jobs when Force parameter is used.
        * The listall command has a new optional parameter status that can be used to filter the job list for jobs with a specific status value.
        * Job completion emails now have two email lists: one for successes and one for failures.
