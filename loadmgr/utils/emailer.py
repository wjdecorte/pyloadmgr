"""
loadmgr/utils/emailer - Contains the function send_email

@author - jwd3

@license:    Apache License 2.0

@contact:    jason.decorte@equifax.com

Version History:
0.01 jwd3 04/13/2017 v1.2.4
    Initial version
0.02 jwd3 05/25/2017 v2.1.2
    Added EmailSettings for smtp_host, smtp_port, from_domain
    Removed mailhost as a parameter
    Changed versioning to 0.xx where xx is a sequential number
"""
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from email.utils import COMMASPACE
from getpass import getuser

from loadmgr import cfg

__version__ = "0.02"
__date__ = '4/13/2017'
__updated__ = '05/25/2017'
__all__ = []


def send_email(recipients, subject, message_text, message_html=None, files=[]):
    """
    Send an email
    :param recipients: List of a recipient's email addresses
    :param subject: email subject
    :param message_text: text version of the email body
    :param message_html: HTML version of the email body
    :param files: List of files to attach to the email
    :return:
    """
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    smtp_host = cfg.EmailSettings.smtp_host or 'localhost'
    smtp_port = cfg.EmailSettings.smtp_port or smtplib.SMTP_PORT
    from_domain = cfg.EmailSettings.from_domain or smtp_host
    msg['From'] = "{0}@{1}".format(getuser(), from_domain)
    msg['To'] = COMMASPACE.join(recipients)
    msg.attach(MIMEText(message_text, 'plain'))
    if message_html:
        msg.attach(MIMEText(message_html, 'html'))
    for f in files:
        with open(f, 'rb') as afile:
            part = MIMEApplication(afile.read(), name=os.path.basename(f))
            part['Content-Disposition'] = 'attachment; filename="{0}"'.format(os.path.basename(f))
            msg.attach(part)
    s = smtplib.SMTP(smtp_host, smtp_port)
    s.sendmail(msg.get('From'), recipients, msg.as_string())
    s.quit()
