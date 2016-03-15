#!/usr/bin/python
from __future__ import print_function
import json
from fusionpy import FusionError
from urllib import urlencode
from os import listdir
from os.path import isfile, join
from string import Template
from connectors import FusionRequester

__author__ = 'jscarbor'


class FusionCollection(FusionRequester):
    """
    A FusionCollection provides access to a collection in Fusion.
    """

    def __init__(self, fusion_instance, collection_name):
        super(FusionCollection, self).__init__(fusion_instance)
        self.collection_name = collection_name
        self.collection_data = None

    def request(self, method, path, headers=None, fields=None, body=None, validate=None):
        if path.find("$") >= 0:
            path = Template(path).safe_substitute(collection=self.collection_name)
        return super(FusionCollection, self).request(method, path, headers, fields, body, validate)

    def collection_exists(self):
        # curl -D - -u admin:dog8code  http://localhost:8764/api/apollo/collections/
        try:
            resp = self.request('GET', "collections/$collection")
            return True
        except FusionError as fe:
            if fe.response.status == 404:
                return False
            else:
                raise fe

    def delete_collection(self, purge=False, solr=False):
        self.request('DELETE',
                     'collections/$collection?' + urlencode({"purge": purge, "solr": solr}))

    def ensure_collection(self, collection, schema, files=None, write=True):
        """
        Idempotent initialization of the collection, only writing new or changed things, not deleting.

        :param collection: a definition of how to instantiate the collection.
        :param schema: a dict containing an element named "fields" which contains an array of dicts as returned by schema()
           No fields will be removed with this operation.  Fields will be added or replaced as necessary to make the
           fields in the collection's schema match with the fields in this parameter.  "fieldTypes" will be processed similarly.
        :param files: if specified, a string containing the name of the folder in which to find files to
           be synced to the solr-config.
        :param write: False to test if the configuration differs from expected, True to set the config.
        :return: self (evaluates as True) if the collection is ready, None (evaluates as False) if it does not exist,
           False otherwise
        """
        # Make sure the collection exists
        if not self.collection_exists():
            if write:
                self.create_collection(collection_config=collection)
            else:
                return None

        # Update solr-config
        if files is not None:
            if not ConfigFiles(self).ensure(files, write=write) and not write:
                return False

        # Update field types
        old_schema = self.schema()
        if "fieldTypes" in schema:
            if not FieldTypes(self).ensure(schema, old_schema, write=write) and not write:
                return False

        # Update fields
        if "fields" in schema:
            if not Fields(self).ensure(schema, old_schema, write=write) and not write:
                return False

        return self

    def create_collection(self, collection_config=None):
        """
        Create this collection
        :param collection_config: a dict with parameters per https://doc.lucidworks.com/fusion/2.1/REST_API_Reference/Collections-API.html#CollectionsAPI-Create,List,UpdateorDeleteCollections
        :return: self, or if the collection already exists, FusionError
        """
        if collection_config is None:
            collection_config = {"solrParams": {"replicationFactor": 1, "numShards": 1}}
        self.request('PUT',
                     "collections/$collection", body=collection_config)
        return self

    def ensure_exists(self):
        """
        Convenience method to create a collection with default options if it doesn't exist
        :return: self
        """
        if not self.collection_exists():
            self.create_collection()
        return self

    def stats(self):
        resp = self.request('GET',
                            'collections/$collection/stats')
        return json.loads(resp.data)

    def clear_collection(self):
        if self.stats()["documentCount"] > 0:
            resp = self.request('POST',
                                'solr/$collection/update?commit=true',
                                body={"delete": {"query": "*:*"}})

    def __query(self, qurl, handler="select", qparams=None):
        qp = {}
        if qparams is not None:
            qp.update(qparams)
        if "wt" not in qp:
            qp["wt"] = "json"
        resp = self.request('GET', qurl + "/" + handler + '?' + urlencode(qp))

        return json.loads(resp.data)

    def query(self, handler="select", pipeline="default", qparams=None, **__qp1):
        if qparams is None:
            qparams = {}
        qparams.update(__qp1)
        return self.__query(qurl='query-pipelines/%s/collections/$collection' % pipeline,
                            handler=handler, qparams=qparams)

    def solrquery(self, handler="select", qparams=None, **__qp1):
        if qparams is None:
            qparams = {}
        qparams.update(__qp1)
        return self.__query(qurl='solr/$collection', handler=handler, qparams=qparams)

    def commit(self):
        self.index({'commit': {}})

    def index(self, docs, pipeline="default"):
        resp = self.request('POST', 'index-pipelines/%s/collections/$collection/index' %
                            pipeline,
                            body=docs
                            )
        wrote = len(json.loads(resp.data))
        if wrote != len(docs):
            raise FusionError(resp,
                              message="Submitted %d documents to index, but wrote %d" % (len(docs), wrote))

    def schema(self):
        resp = self.request('GET', "solr/$collection/schema" )
        return json.loads(resp.data)["schema"]


class AbstractFieldsConfig(FusionRequester):
    def __init__(self, collection, fctype):
        super(AbstractFieldsConfig, self).__init__(collection)
        self.collection = collection
        self.fctype = fctype

    def ensure(self, schema, old_schema=None, write=True):
        """
        :param schema: desired elements of schema to add or modify
        :param old_schema: for expediency, the collection's schema
        :param write: True if changes should be written, False to check if changes are in order
        :return: False if a change was in order
        """
        if old_schema is None:
            old_schema = self.collection.get_schema()

        old_map = {}
        for old_f in old_schema[self.fctype]:
            old_map[old_f["name"]] = old_f

        configured = True
        for new_f in schema[self.fctype]:
            ftn = new_f["name"]
            if ftn in old_map:
                if cmp(new_f, old_map[ftn]) != 0:
                    configured = False
                    if write:
                        self.update(new_f)
            else:
                configured = False
                if write:
                    self.add(new_f)
            if not configured and not write:
                # If we found a change and aren't writing, then return True
                return False
        return configured

    def change_field(self, action, field_descriptor):
        """
        Add, delete, or replace a field declaration.

        :param field_descriptor:
        :param action: One of "add", "delete", or "replace"
        :return: self

        See https://cwiki.apache.org/confluence/display/solr/Schema+API#SchemaAPI-AddaNewField
        """
        if action not in ["add", "delete", "replace"]:
            raise ValueError("Invalid action")

        if self.fctype == "fieldTypes":
            action += "-field-type"
        else:
            action += "-field"

        self.request('POST',
                     "solr/$collection/schema",
                     body={action: field_descriptor},
                     validate=lambda resp: "errors" not in json.loads(resp.data))

        return self

    def update(self, definition):
        self.change_field("replace", definition)

    def add(self, definition):
        self.change_field("add", definition)


class Fields(AbstractFieldsConfig):
    def __init__(self, collection):
        super(Fields, self).__init__(collection, 'fields')


class FieldTypes(AbstractFieldsConfig):
    def __init__(self, collection):
        super(FieldTypes, self).__init__(collection, 'fieldTypes')


class ConfigFiles(FusionRequester):
    def __init__(self, collection):
        super(ConfigFiles, self).__init__(collection)

    def ensure(self, files, write=True):
        # one day this could support (base64?) encoded files within the json if it's not a path
        configured = True
        for f in [f for f in listdir(files) if isfile(join(files, f))]:
            basename = f.rsplit('/', 1)[-1]
            with open(join(files, f), "r") as fh:
                configured &= self.set_config_file(basename, fh.read(), write=write)
                if not write and not configured:
                    # It hasn't actually written, but has detected a difference.
                    return False
        return configured

    def config_files(self):
        resp = json.loads(
            self.request(
                'GET',
                "collections/$collection/solr-config"))
        rd = json.loads(resp.data)
        if "errors" in rd:
            raise FusionError(resp)
        return rd

    def get_config_file(self, filename):
        resp = self.request('GET',
                            "collections/$collection/solr-config/%s" %
                            filename)
        return resp.data

    def set_config_file(self, filename, contents, content_type="application/xml", reload=True, write=True):
        """
        Create or update a config file.  If the server's file is the same as the provided file,
        the write is skipped.

        :param filename: The name of the file
        :param contents: The body of the file
        :param content_type: The file's content type
        :param reload: True to request solr reload after the file is populated
        :param write: True if this method should perform the write, false for inspection only
        :return: True if the file was written (or would have been), false otherwise
        """
        # https://doc.lucidworks.com/fusion/2.1/REST_API_Reference/Solr-Configuration-API.html#SolrConfigurationAPI-CreateorUpdateaFileinZooKeeper

        # Select the correct method
        try:
            oldfile = self.get_config_file(filename)
            method = 'PUT'  # PUT a file to change
            if oldfile == contents:
                # no change needed
                return False
        except FusionError as e:
            if e.response.status == 404:
                # POST a new file
                method = 'POST'
            else:
                raise e

        # submit the file
        if write:
            self.request(
                method,
                "collections/$collection/solr-config/%s?%s" %
                (filename,
                 urlencode({"reload": reload})),
                headers={"Content-Type": content_type},
                body=contents)
        return True
