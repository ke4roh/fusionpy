#!/usr/bin/python
from __future__ import print_function

__all__ = ['Fusion', 'FusionCollection', 'FusionError', 'FusionRequester', 'HttpFusionRequester']

class FusionError(IOError):
    def __init__(self, response, request_body=None, message=None, url=None):
        """
        :param response: The HTTP response, having attributes .body and .status (or str or unicode)
        :param request_body: The HTTP request body that percipitated this error
        :param message: Any text to go along with this
        :param url: The URL requested
        """
        if response.__class__ is str or response.__class__ is unicode:
            if message is None:
                message = response
            else:
                message += response
            response = None
        if message is None:
            message = ""
            if url is not None:
                message = "Requested " + url + "\n"
            if request_body is not None:
                message += request_body
            if response is not None:
                message += "Status %d\n\n%s" % (response.status, response.data)
        IOError.__init__(self, message)
        self.response = response
        self.url = url
