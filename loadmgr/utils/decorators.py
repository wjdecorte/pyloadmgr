"""
Created on Jan 8, 2015

@author: jwd3

Version History:
0.2 jwd 11/16/2015
    Added functools.wrap for decorators
"""
import functools
__version__ = "0.2"
__date__ = '2015-01-08'
__updated__ = '2015-11-16'


class DBRequest(object):
    """
    Add database connection and close calls around a function/method
    """
    
    def __init__(self, db):
        """
        Constructor
        """
        self.db = db
        
    def __call__(self, func):
        """
        Pass the function to the call method and wrap it
        """
        @functools.wraps(func)
        def wrapped_func(*args,**kwargs):
            self.db.connect()
            rv = func(*args,**kwargs)
            self.db.close()
            return rv
        return wrapped_func


class LogEntryExit(object):
    """
    Add enter/exit debug statements for a function/method
    """
    
    def __init__(self,logger_instance):
        """
        Constructor
        """
        self.logger = logger_instance
        
    def __call__(self, func):
        """
        Pass the function to the call method and wrap it
        """
        @functools.wraps(func)
        def wrapped_func(*args,**kwargs):
            self.logger.debug("Entering {0}".format(func.__name__))
            rv = func(*args,**kwargs)
            self.logger.debug("Exiting {0}".format(func.__name__))
            return rv
        return wrapped_func
