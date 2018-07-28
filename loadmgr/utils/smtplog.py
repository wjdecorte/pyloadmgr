"""
Created on Sep 2, 2014

@author: jwd3

Version History
0.01 jwd 03/06/2015
    Added ability to have two recipient email lists, success and failure
    Converted version to string from float
0.02 jwd 11/12/2015
    Removed Raise Exception in Try statement of flush method
    Add sys.stderr.write if failure on send email
0.03 jwd3 05/25/2017
    Updated versioning to 0.xx where xx is a sequential number
    Added config parameters for smtp_host, smtp_port and from_domain
0.04 jwd3 05/27/2017
    Added constant for logging level instead of numeric value
0.05 jwd3 06/21/2017 v2.2.1
    Removed getSubject override.
    Added check for empty buffer
    Added on_failure_only option - send email only in the case of failure
"""

__version__ = "0.05"
__date__ = '2014-09-02'
__updated__ = '06/21/2017'

import logging.handlers
from logging import WARNING
from getpass import getuser
import smtplib
from email.utils import formatdate

from loadmgr import cfg


class BufferingSMTPHandler(logging.handlers.SMTPHandler):
    """
    Buffer log records until ready to send and then mail them to specified address(es)
    """

    def __init__(self, toaddrs, subject, on_failure_only=False):
        """
        Constructor
        """
        user = getuser()
        smtp_host = cfg.EmailSettings.smtp_host or 'localhost'
        from_domain = cfg.EmailSettings.from_domain or smtp_host
        sender = "{0}@{1}".format(user,from_domain)
        self.new_toaddrs = []
        logging.handlers.SMTPHandler.__init__(self,smtp_host,sender,toaddrs,subject)
        self.buffer = []
        self.on_failure_only = on_failure_only

    def emit(self, record):
        """
        Store log record
        """
        self.buffer.append(record)

    def flush(self):
        """
        Flush the buffer by sending records in buffer to recipients
        """
        if self.buffer:
            # look for any error or critical messages in buffer to determine status
            failure = len(filter(lambda x: x.levelno > WARNING,self.buffer)) > 0
            if (self.on_failure_only and failure) or not self.on_failure_only:
                # if failure found, then set TO Address to failure email list else set to success email list
                self.new_toaddrs = self.toaddrs.get('failure') if failure else self.toaddrs.get('success')
                if self.subject.count('{status}') > 0:
                    # place holder for status exists in subject
                    status = 'FAILURE' if failure else 'SUCCESS'
                    self.subject = self.subject.format(status=status)

                port = cfg.EmailSettings.smtp_port or smtplib.SMTP_PORT
                smtp = smtplib.SMTP(self.mailhost, port)
                # format each record in the buffer and join each formatted record using \r\n
                body = '\r\n'.join(map(lambda x: self.format(x),self.buffer))
                msg = "From: %s\r\nTo: %s\r\nSubject: %s\r\nDate: %s\r\n\r\n%s" % (self.fromaddr,
                                                                                   ",".join(self.new_toaddrs),
                                                                                   self.subject,
                                                                                   formatdate(localtime=True),
                                                                                   body)
                if self.username:
                    smtp.login(self.username, self.password)
                smtp.sendmail(self.fromaddr, self.new_toaddrs, msg)
                smtp.quit()
