#!/usr/bin/python
from __future__ import print_function
import os
import json
import urllib3
from urlparse import urlparse
from base64 import b64encode
import fusionpy
from fusionpy.fusioncollection import FusionCollection

SYS_IX_PIPELINES_START = ['_aggr', '_signals_ingest', '_system']


class Fusion:
    def __init__(self, fusion_url=None, urllib3_pool_manager=None):
        """
        :param fusion_url: The URL to a collection in Fusion, None to use os.environ["FUSION_API_COLLECTION_URL"]
        :param urllib3_pool_manager: urllib3.PoolManager() by default.  Anything duckwise-compatible.
        :return: a Fusion object whose ping responds successfully
        """
        if fusion_url is None:
            fusion_url = os.environ.get('FUSION_API_COLLECTION_URL',
                                        'http://admin:topSecret5@localhost:8764/api/apollo/collections/mycollection')
        if urllib3_pool_manager is None:
            self.http = urllib3.PoolManager()
        else:
            self.http = urllib3_pool_manager
        self.fusion_url_parsed = fusion_url_parsed = urlparse(fusion_url)
        self.hostname = fusion_url_parsed.hostname
        self.port = fusion_url_parsed.port
        self.url = '%s://%s:%d' % (fusion_url_parsed.scheme, self.hostname, self.port)
        self.credentials = b64encode('%s:%s' % (fusion_url_parsed.username, fusion_url_parsed.password))
        self.default_collection = fusion_url_parsed.path.rsplit('/', 1)[-1]
        self.api_url = self.url + '/'.join(fusion_url_parsed.path.split('/', 3)[0:3]) + '/'
        self.ping()

    def ping(self):
        """
        :return: True if the syetem is initialized, false if the admin password is not yet set, FusionError if
            the server doesn't respond or all its services aren't working correctly.
        """
        try:
            resp = self.http.request('GET', self.url + '/api')
        except urllib3.exceptions.MaxRetryError as mre:
            raise fusionpy.FusionError(None, message="Fusion port %d isn't working. %s" % (self.port, str(mre)))

        if resp.status > 200:
            raise fusionpy.FusionError(resp, message="Fusion is not responding to status checks.")

        rd = json.loads(resp.data)
        for thing, stats in rd["status"].items():
            notworking = []
            if "ping" in stats and not stats["ping"]:
                notworking.append(thing)
            if len(notworking) > 0:
                raise fusionpy.FusionError(resp, "Fusion services %s are not working." % str(notworking))

        return rd["initMeta"] is not None

    def ensure_config(self, collections=None, queryPipelines=None, indexPipelines=None):
        """
        Idempotent initialization of the configuration according to params
        :param collections: a list of collection configurations. see fusioncollection.FusionCollection.createCollection
        :param queryPipelines: a list of query pipelines
        :param indexPipelines: a list of index pipelines
        :return: self
        """
        assert self.ping(), "Configure the admin password"
        if collections is not None:
            for c, ccfg in collections.iteritems():
                self.get_collection(c).ensure_collection(**ccfg)

        if queryPipelines is not None:
            pass
        if indexPipelines is not None:
            pass

        return self

    def get_collection(self, collection=None):
        """Return a FusionCollection for querying, posting, and such"""
        if collection is None or collection == "__default":
            collection = self.default_collection
        return FusionCollection(self, collection)

    def set_admin_password(self, password=None):
        if password is None:
            if self.fusion_url_parsed.username == "admin":
                password = self.fusion_url_parsed.password
            else:
                raise fusionpy.FusionError(None, message="No admin password supplied")

        url = self.url + '/api'
        headers = {"Content-Type": "application/json"}
        body = json.dumps({"password": password})
        resp = self.http.request('POST', url, headers=headers,
                                 body=body)
        if resp.status != 201:
            raise fusionpy.FusionError(resp)

    def __request(self, method, path, headers=None, fields=None, body=None):
        """
        Send an authenticated request to the API.
        :param method: 'GET', 'PUT', 'POST', etc.
        :param path: the part after "/api/apollo/" (note that it must not include a leading slash)
        :param headers: anything besides Authorization that may be necessary
        :param fields: to include in the request, for requests that are not POST or HEAD,
           these will be encoded on the URL
        :param body: for submitting with the request.  Body type should be string, bytes, list, or dict.  For the
            latter two, they will be encoded as json and the Content-Type header set to "application/json".
        :return: response if response.status is in the 200s, FusionError containing the response body otherwise
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

        url = self.api_url + path
        resp = self.http.request(method, url, headers=h, fields=fields, body=body)

        if resp.status < 200 or resp.status > 299:
            raise fusionpy.FusionError(resp, url=url)
        return resp

    def get_index_pipelines(self, include_system=False):
        pl = json.loads(self.__request('GET', 'index-pipelines').data)
        return [x for x in pl if
                include_system or
                len([y for y in SYS_IX_PIPELINES_START if x['id'].startswith(y)]) == 0]

    def get_query_pipelines(self, include_system=False):
        pl = json.loads(self.__request('GET', 'query-pipelines').data)
        return [x for x in pl if
                include_system or not x['id'].startswith('system_')]

    def add_query_pipelines(self, queryPipeline):
        # https://doc.lucidworks.com/fusion/2.1/REST_API_Reference/Query-Pipelines-API.html
        pass
