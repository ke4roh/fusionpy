#!/usr/bin/python
import json
from fusionpy import FusionError
from fusionpy.fusioncollection import FusionCollection
from fusionpy.connectors import FusionRequester, HttpFusionRequester
import re
import os
import errno


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
                raise

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

    def export_config(self, things_to_save, config_file_path="fusion-config/"):
        system_config = {}
        if 'collections' in things_to_save:
            system_config["collections"] = {}
            for c in things_to_save['collections']:
                fc = self.get_collection(c)
                system_config["collections"][c] = {}
                system_config["collections"][c]['collection'] = fc.get_config()
                path = config_file_path + c
                mkdir_p(path)
                system_config["collections"][c]["files"] = path
                for cf in [x["name"] for x in fc.config_files.dir() if
                           x["name"] != "managed-schema" and
                                   not x['isDir'] and
                                   (x['version'] > 0
                                    or x["name"] not in ["currency.xml",
                                                         "elevate.xml",
                                                         "params.json",
                                                         "protwords.txt",
                                                         "solrconfig.xml",
                                                         "stopwords.txt",
                                                         "synonyms.txt"])]:
                    with open(path + "/" + cf, "w") as fh:
                        fh.write(fc.config_files.get_config_file(cf))
                schema = fc.schema()
                system_config["collections"][c]['schema'] = {}
                system_config["collections"][c]['schema']["fields"] = schema["fields"]
                system_config["collections"][c]['schema']["fieldTypes"] = schema["fieldTypes"]

        if 'indexPipelines' in things_to_save:
            system_config["indexPipelines"] = [p for p in self.index_pipelines.get_pipelines() if
                                               p["id"] in things_to_save['indexPipelines']]
        if 'queryPipelines' in things_to_save:
            system_config["queryPipelines"] = [p for p in self.query_pipelines.get_pipelines() if
                                               p["id"] in things_to_save['queryPipelines']]

        print json.dumps(system_config, indent=True, separators=(',', ':'), sort_keys=True)

    def get_collections(self, include_system=False):
        """
        :param include_system: True if system collections should be included. Default is False
        :return: A list of the names of the collections
        """
        system_collections = re.compile('_signals$|_signals_aggr$|^system_|_logs$|^logs$')
        collections = []
        for c in json.loads(self.request('GET', 'collections/').data):
            if include_system or not system_collections.search(c["id"]):
                collections.append(c["id"])
        return collections

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


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise
