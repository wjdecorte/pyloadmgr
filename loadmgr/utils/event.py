"""
loadmgr.utils.event.py - Event related Utility Functions

@author - jwd3

@license:    Apache License 2.0

@contact:    jason.decorte@equifax.com

Version History:
0.2 jwd3 03/08/2017 HotFix
    Added generic exception handling to capture any exception
"""
# built-in
import json
from datetime import datetime, date
import logging
import sys
import os

# third-party
import requests

from loadmgr import cfg

__version__ = "0.2"
__date__ = '2/19/2017'
__updated__ = '03/08/2017'
__all__ = []

# Get url and token for create_event
event_api_url = "http://{host}:{port}/{endpoint}".format(host=cfg.event_api_conn.host,
                                                         port=cfg.event_api_conn.port,
                                                         endpoint=cfg.event_api_conn.endpoint)
event_api_token = cfg.event_api_conn.token

logger = logging.getLogger(os.path.basename(sys.argv[0]).split(".")[0] + ".create_event")


def create_event(event, print_results=False):
    """
    Create event using passed dictionary, adding current time in UTC
    :param event: Dictionary of event attributes
    :param print_results:  Optional, print result of response
    :return: None
    """
    logger.debug("Enter")

    # use custom encoder to handle dates and datetimes
    class DatetimeEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (date, datetime)):
                return str(obj)
            return json.JSONEncoder.default(self, obj)

    # Add/Overwrite date/time in UTC
    event['event_dttm'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    logger.debug("set event_dttm to UTC now")
    try:
        r = requests.post(event_api_url,
                          data=json.dumps(event, separators=(',', ':'), cls=DatetimeEncoder),
                          headers={'content-type': 'application/json'},
                          auth=(event_api_token, ''),
                          timeout=cfg.EVENT_API_TIMEOUT)
        if print_results:
            logger.info("{0} - {1}".format(r.status_code, r.reason))
    except:
        if print_results:
            logger.info("Connection Timeout or Error")
        logger.debug("Exiting")
