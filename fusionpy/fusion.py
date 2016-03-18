#!/usr/bin/python
import json
from fusionpy import FusionError
from fusionpy.fusioncollection import FusionCollection
from fusionpy.connectors import FusionRequester, HttpFusionRequester


class Fusion(FusionRequester):
    def __init__(self, requester=None):
        """
        :param requester: The class responsible for managing connections.  A FusionRequester or HttpFusionRequester
        :return: a Fusion object whose ping responds successfully
        """
        if requester is None:
            requester = HttpFusionRequester()
        super(Fusion, self).__init__(requester)
        self.ping()
        self.index_pipelines = IndexPipelines(self)
        self.query_pipelines = QueryPipelines(self)

    def ping(self):
        """
        :return: True if the syetem is initialized, false if the admin password is not yet set, FusionError if
            the server doesn't respond or all its services aren't working correctly.
        """
        try:
            resp = self.request('GET', '/api')
        except FusionError as fe:
            if fe.response is not None and fe.response.status > 200:
                raise FusionError(fe.response, message="Fusion is not responding to status checks.")
            else:
                raise fe

        rd = json.loads(resp.data)
        for thing, stats in rd["status"].items():
            not_working = []
            if "ping" in stats and not stats["ping"]:
                not_working.append(thing)
            if len(not_working) > 0:
                raise FusionError(resp, "Fusion services %s are not working." % str(not_working))

        return rd["initMeta"] is not None

    def ensure_config(self, collections=None, queryPipelines=None, indexPipelines=None, write=True):
        """
        Idempotent initialization of the configuration according to params
        :param collections: a list of collection configurations. see fusioncollection.FusionCollection.createCollection
        :param queryPipelines: a list of query pipelines
        :param indexPipelines: a list of index pipelines
        :param write: False to only report if changes are required
        :return: self if everything is ready, None if the admin password is not set or a collection is absent,
                 False if there is some configuration change outstanding
        """
        if not self.ping():
            if write:
                self.set_admin_password()
                if not self.ping():
                    raise FusionError(None, message="Configure the admin password")
            else:
                return None

        if collections is not None:
            for c, ccfg in collections.iteritems():
                cr = self.get_collection(c).ensure_collection(write=write, **ccfg)
                if not write and not cr:
                    return cr

        if queryPipelines is not None:
            if not self.query_pipelines.ensure_config(queryPipelines, write=write):
                return False

        if indexPipelines is not None:
            if not self.index_pipelines.ensure_config(indexPipelines, write=write):
                return False

        return self

    def get_collection(self, collection=None):
        """Return a FusionCollection for querying, posting, and such"""
        if collection is None or collection == "__default":
            collection = self.get_default_collection()
        return FusionCollection(self, collection)

    def set_admin_password(self, password=None):
        if password is None:
            password = self.get_admin_password()
            if password is None:
                raise FusionError(None, message="No admin password supplied")

        resp = self.request('POST', "/api", body={"password": password})
        if resp.status != 201:
            raise FusionError(resp)


class Pipelines(FusionRequester):
    def __init__(self, fusion_instance):
        super(Pipelines, self).__init__(fusion_instance)
        tn = type(self).__name__
        self.ptype = tn[0:len(tn) - 9].lower()  # "query" or "index"

    def ensure_config(self, config_pipelines, write=True):
        fusion_pipelines_list = self.get_pipelines()
        fusion_pipelines_map = {}
        for p in fusion_pipelines_list:
            fusion_pipelines_map[p['id']] = p
        for p in config_pipelines:
            if p['id'] in fusion_pipelines_map:
                if cmp(p, fusion_pipelines_map[p['id']]) != 0:
                    if write:
                        self.update_pipeline(p)
                    else:
                        return False
            else:
                if write:
                    self.add_pipeline(p)
                else:
                    return False
        return True

    def get_pipelines(self):
        return json.loads(self.request('GET', self.ptype + '-pipelines').data)

    def add_pipeline(self, pipeline):
        self.request('POST', self.ptype + '-pipelines/', body=pipeline)

    def update_pipeline(self, pipeline):
        pid = pipeline['id']
        self.request('PUT', self.ptype + '-pipelines/' + pid, body=pipeline)
        self.request('PUT', self.ptype + '-pipelines/' + pid + '/refresh')


class QueryPipelines(Pipelines):
    def __init__(self, fusion_instance):
        super(QueryPipelines, self).__init__(fusion_instance)

    def get_pipelines(self, include_system=False):
        return [x for x in super(QueryPipelines, self).get_pipelines() if
                include_system or not x['id'].startswith('system_')]


class IndexPipelines(Pipelines):
    def __init__(self, fusion_instance):
        super(IndexPipelines, self).__init__(fusion_instance)

    def get_pipelines(self, include_system=False):
        return [x for x in super(IndexPipelines, self).get_pipelines() if
                include_system or
                len([y for y in ['_aggr', '_signals_ingest', '_system'] if x['id'].startswith(y)]) == 0]
