"""
Created on August 14, 2014

@author: jwd3

Version History:
0.01 jwd 03/06/2015
    Added initial doc section
    Remove patch list
    Cleanup old commented-out code
    Converted version to string from float
0.02 jwd 6/17/2015
    Code has baked for several months.  Official release with some enhancements and bug fixes
    Added check for user credentials.
    Changed check for user credentials matches owner of config file as adminuser
0.03 jwd 10/21/2015
    Added MySQL support
    Added functions to efxldr to support file-based ingestion
    Minor bug fixes
    Update repo version
0.04 jwd3 01/28/2016
    Depracated gencfg by moving code to this module
    Added version, date and updated variables
0.05 jwd3 02/18/2017
    Changed master version to 1.2
0.06 jwd3 02/22/2017
    Changed master version to 1.2.1
0.07 jwd3 03/08/2017
    Changed master version to 1.2.2
0.08 jwd3 03/21/2017
    Changed master version to 1.2.3
0.09 jwd3 04/13/2017
    Changed master version to 1.2.4
0.10 jwd3 04/17/2017
    Changed master version to 1.3.1
0.11 jwd3 05/11/2017
    Versioning changed to 0.xx where xx is sequential number representing each change to this file
        advanced versioning with major/minor isn't needed
    Changed master version to 2.0.0
    Added creation of default config file if one isn't found
0.12 jwd3 05/20/2017
    Changed master_version to 2.1.0
0.13 jwd3 05/21/2017
    Changed master_version to 2.1.1
0.14 jwd3 05/22/2017
    Changed master_version to 2.1.2
0.15 jwd3 06/06/2017
    Changed master version to 2.1.3
0.16 jwd3 06/06/2017
    Changed master version to 2.1.4
0.17 jwd3 06/15/2017
    Changed master version to 2.2.1
"""
#initialize configuration
import sys
import os
from config import Config
from getpass import getuser
from metadata.constants import CONFIG_ENV_VAR,DEFAULT_CONFIG_FILENAME,DEFAULT_CONFIG_FILE

__version__ = "0.17"
__date__ = '2014-08-14'
__updated__ = '06/15/2017'

if os.getenv(CONFIG_ENV_VAR):
    config_file_name = os.path.expandvars(os.getenv(CONFIG_ENV_VAR))
else:
    config_file_name = os.path.join(os.getcwd(),DEFAULT_CONFIG_FILENAME)
if not os.path.isfile(config_file_name):
    # write default file
    with open(config_file_name,'w') as default_file:
        default_file.write(DEFAULT_CONFIG_FILE)
with open(config_file_name,'r') as config_file:
    cfg = Config(config_file)

if not cfg:
    sys.stderr.write("CRITICAL ERROR:  Unable to find configuration file\n")
    sys.exit(1)

# check user credentials
if sys.platform == 'linux2':
    #adminuser = 660 in os.getgroups()
    # Check if user executing loadmgr is the same owner that owns the config file
    adminuser = os.geteuid() == os.stat(config_file_name).st_uid
elif sys.platform == 'win32':
    user = getuser()
    adminuser = user in ['jwd3']
else:
    adminuser = False

master_version = "2.2.1"
repository_version = "1.1.0"
