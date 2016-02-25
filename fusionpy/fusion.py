#!/usr/bin/python
from __future__ import print_function
import os
import sys
import json
import urllib3
from urlparse import urlparse
from base64 import b64encode
import fusionpy
from fusionpy.fusioncollection import FusionCollection

# Keep up with just one connection pool
http = fusionpy.http


class Fusion:
    def __init__(self, fusion_url=None):
        """
        :param fusion_url: The URL to a collection in Fusion, None to use os.environ["FUSION_API_COLLECTION_URL"]
        :return: a Fusion object whose ping responds successfully
        """
        if fusion_url is None:
            fusion_url = os.environ.get('FUSION_API_COLLECTION_URL',
                                        'http://admin:topSecret5@localhost:8764/api/apollo/collections/mycollection')
        fusion_url_parsed = urlparse(fusion_url)
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
            resp = http.request('GET', self.url + '/api')
        except urllib3.exceptions.MaxRetryError as mre:
            raise fusionpy.FusionError(None, message="Fusion port 8764 isn't working. " + str(mre))

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

    def get_collection(self, collection=None):
        """Return a FusionCollection for querying, posting, and such"""
        if collection is None:
            collection = self.default_collection
        return FusionCollection(self, collection)

    def add_query_pipelines(self, queryPipeline):
        # https://doc.lucidworks.com/fusion/2.1/REST_API_Reference/Query-Pipelines-API.html
        pass
