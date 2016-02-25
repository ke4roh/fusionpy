#!/usr/bin/python
from __future__ import print_function
import urllib3

http = urllib3.PoolManager()
__all__ = ['Fusion', 'FusionCollection', 'FusionError']


class FusionError(IOError):
    def __init__(self, response, message=None, url=None):
        if message is None:
            message = ""
            if url is not None:
                message = "Requested " + url + "\n"
            message += "Status %d\n\n%s" % (response.status, response.data)
        IOError.__init__(self, message)
        self.response = response
        self.url = url
