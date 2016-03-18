import json
from urllib import urlencode
import urllib3
from urlparse import urlparse
from base64 import b64encode
from fusionpy import FusionError
import os

"""
Contains Requesters - classes with functions for managing connections to Fusion
"""

class FusionRequester(object):
    """
    Default delegating requester.
    """
    def __init__(self, request_handler):
        self.request_handler = request_handler

    def get_admin_password(self):
        return self.request_handler.get_admin_password()

    def get_default_collection(self):
        return self.request_handler.get_default_collection()

    def request(self, method, path, headers=None, fields=None, body=None, validate=None):
        return self.request_handler.request(method, path, headers, fields, body, validate)

class HttpFusionRequester(object):
    """
    Running requests through HTTP
    """
    def __init__(self, fusion_url=None, urllib3_pool_manager=None):
        if fusion_url is None:
            fusion_url = os.environ.get('FUSION_API_COLLECTION_URL',
                                'http://admin:topSecret5@localhost:8764/api/apollo/collections/mycollection')

        self.fusion_url_parsed = fusion_url_parsed = urlparse(fusion_url)
        self.hostname = fusion_url_parsed.hostname
        self.port = fusion_url_parsed.port
        self.url = '%s://%s:%d' % (fusion_url_parsed.scheme, self.hostname, self.port)
        self.credentials = b64encode('%s:%s' % (fusion_url_parsed.username, fusion_url_parsed.password))
        self.default_collection = fusion_url_parsed.path.rsplit('/', 1)[-1]
        self.api_url = self.url + '/'.join(fusion_url_parsed.path.split('/', 3)[0:3]) + '/'

        if urllib3_pool_manager is None:
            self.http = urllib3.PoolManager()
        else:
            self.http = urllib3_pool_manager

    def get_admin_password(self):
        if self.fusion_url_parsed.username == "admin":
            return self.fusion_url_parsed.password
        else:
            return None

    def get_default_collection(self):
        return self.fusion_url_parsed.path.rsplit('/', 1)[-1]

    def request(self, method, path, headers=None, fields=None, body=None, validate=None):
        """
        Send an authenticated request to the API.
        :param method: 'GET', 'PUT', 'POST', etc.
        :param path: the part after "/api/apollo/", or if it starts with a slash, the whole path after the hostname
        :param headers: anything besides Authorization that may be necessary
        :param fields: to include in the request, for requests that are not POST or HEAD,
           these will be encoded on the URL
        :param body: for submitting with the request.  Body type should be string, bytes, list, or dict.  For the
            latter two, they will be encoded as json and the Content-Type header set to "application/json".
        :param validate: A function taking one parameter, the response, which can be inspected. The function returns
            true if the response is valid, false otherwise.
        :return: response if response.status is in the 200s, FusionError containing the response otherwise
        """
        h = {"Authorization": "Basic " + self.credentials,
             "Accept": "application/json; q=1.0, text/plain; q=0.7, application/xml; q=0.5, */*; q=0.1"}
        if headers is not None:
            h.update(headers)

        if fields is not None and method != 'POST' and method != 'HEAD':
            path += '?' + urlencode(fields)
            fields = None

        if body is not None and (type(body) is dict or type(body) is list):
            h["Content-Type"] = "application/json"
            body = json.dumps(body)

        if path.startswith('/'):
            url = self.url + path
        else:
            url = self.api_url + path

        try:
            resp = self.http.request(method, url, headers=h, fields=fields, body=body)
        except urllib3.exceptions.MaxRetryError as mre:
            raise FusionError(None, message="Fusion port %d isn't working. %s" % (self.port, str(mre)))

        if resp.status < 200 or resp.status > 299 or (validate is not None and not validate(resp)):
            raise FusionError(resp, request_body=body, url=url)
        return resp
