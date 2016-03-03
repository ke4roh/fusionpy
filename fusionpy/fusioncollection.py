#!/usr/bin/python
from __future__ import print_function
import json
import fusionpy
from urllib import urlencode

__author__ = 'jscarbor'


class FusionCollection:
    """
    A FusionCollection provides access to a collection in Fusion.
    """

    def __init__(self, fusion_instance, collection_name):
        self.fusion_instance = fusion_instance
        self.collection_name = collection_name
        self.collection_data = None
        self.http = fusion_instance.http

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
        h = {"Authorization": "Basic " + self.fusion_instance.credentials,
             "Accept": "application/json; q=1.0, text/plain; q=0.7, application/xml; q=0.5, */*; q=0.1"}
        if headers is not None:
            h.update(headers)

        if fields is not None and method != 'POST' and method != 'HEAD':
            path += '?' + urlencode(fields)
            fields = None

        if body is not None and (type(body) is dict or type(body) is list):
            h["Content-Type"] = "application/json"
            body = json.dumps(body)

        url = self.fusion_instance.api_url + path
        resp = self.http.request(method, url, headers=h, fields=fields, body=body)

        if resp.status < 200 or resp.status > 299:
            raise fusionpy.FusionError(resp, url=url)
        return resp

    def collection_exists(self):
        # curl -D - -u admin:dog8code  http://localhost:8764/api/apollo/collections/
        try:
            resp = self.__request('GET', "collections/" + self.collection_name)
            return True
        except fusionpy.FusionError as fe:
            if fe.response.status == 404:
                return False
            else:
                raise fe

    def delete_collection(self, purge=False, solr=False):
        self.__request('DELETE',
                       'collections/' + self.collection_name + '?' + urlencode({"purge": purge, "solr": solr}))

    def ensure_collection(self, collection, schema, files=None):
        """
        Idempotent initialization of the collection

        :param collection: a definition of how to instantiate the collection.
        :param schema: a dict containing an element named "fields" which contains an array of dicts as returned by schema()
           No fields will be removed with this operation.  Fields will be added or replaced as necessary to make the
           fields in the collection's schema match with the fields in this parameter.  "fieldTypes" will be processed similarly.
        :param files: if specified, a string containing the name of the folder in which to find files to
           be synced to the solr-config.
        :return: self
        """
        # Make sure the collection exists
        if not self.collection_exists():
            self.create_collection(collection_config=collection)

        # Update solr-config
        if files is not None:
            # one day this could support (base64?) encoded files within the json if it's not a path
            from os import listdir
            from os.path import isfile, join

            for f in [f for f in listdir(files) if isfile(join(files, f))]:
                basename = f.rsplit('/', 1)[-1]
                with open(join(files, f), "r") as fh:
                    self.set_config_file(basename, fh.read())

        # Update field types
        old_schema = self.schema()
        if "fieldTypes" in schema:
            old_field_types_map = {}
            for old_ft in old_schema["fieldTypes"]:
                old_field_types_map[old_ft["name"]] = old_ft

            for new_ft in schema["fieldTypes"]:
                ftn = new_ft["name"]
                if ftn in old_field_types_map:
                    if cmp(new_ft, old_field_types_map[ftn]) != 0:
                        self.replace_field_type(new_ft)
                else:
                    self.add_field_type(new_ft)

        # Update fields
        if "fields" in schema:
            old_fields_map = {}
            for old_f in old_schema["fields"]:
                old_fields_map[old_f["name"]] = old_f

            for new_f in schema["fields"]:
                fn = new_f["name"]
                if fn in old_fields_map:
                    if cmp(new_f, old_fields_map[fn]) != 0:
                        self.replace_field(new_f)
                else:
                    self.add_field(new_f)

        return self

    def config_files(self):
        resp = json.loads(self.__request('GET', "collections/%s/solr-config" % self.collection_name))
        rd = json.loads(resp.data)
        if "errors" in rd:
            raise fusionpy.FusionError(resp)
        return rd

    def get_config_file(self, filename):
        resp = self.__request('GET',
                              "collections/%s/solr-config/%s" %
                              (self.collection_name, filename))
        return resp.data

    def set_config_file(self, filename, contents, content_type="application/xml"):
        # https://doc.lucidworks.com/fusion/2.1/REST_API_Reference/Solr-Configuration-API.html#SolrConfigurationAPI-CreateorUpdateaFileinZooKeeper

        # Select the correct method
        try:
            oldfile = self.get_config_file(filename)
            method = 'PUT'  # PUT a file to change
            if oldfile == contents:
                # no change needed
                return
        except fusionpy.FusionError as e:
            if e.response.status == 404:
                # POST a new file
                method = 'POST'
            else:
                raise e

        # submit the file
        resp = self.__request(method,
                              "collections/%s/solr-config/%s" %
                              (self.collection_name, filename),
                              headers={"Content-Type": content_type},
                              body=contents)

    def add_field_type(self, field_type_descriptor):
        return self.change_field("add", field_type_descriptor, is_type=True)

    def replace_field_type(self, field_type_descriptor):
        return self.change_field("replace", field_type_descriptor, is_type=True)

    def add_field(self, field_descriptor):
        return self.change_field("add", field_descriptor)

    def replace_field(self, field_descriptor):
        return self.change_field("replace", field_descriptor)

    def change_field(self, action, field_descriptor, is_type=False):
        """
        Add, delete, or replace a field declaration.

        :param field_descriptor:
        :param action: One of "add", "delete", or "replace"
        :return: self

        See https://cwiki.apache.org/confluence/display/solr/Schema+API#SchemaAPI-AddaNewField
        """
        if action not in ["add", "delete", "replace"]:
            raise ValueError("Invalid action")

        if is_type:
            action += "-field-type"
        else:
            action += "-field"

        resp = self.__request('POST',
                              "solr/%s/schema" % self.collection_name,
                              body={action: field_descriptor})
        if "errors" in json.loads(resp.data):
            raise fusionpy.FusionError(resp)

        return self

    def create_collection(self, collection_config=None):
        """
        Create this collection
        :param collection_config: a dict with parameters per https://doc.lucidworks.com/fusion/2.1/REST_API_Reference/Collections-API.html#CollectionsAPI-Create,List,UpdateorDeleteCollections
        :return: self, or if the collection already exists, FusionError
        """
        if collection_config is None:
            collection_config = {"solrParams": {"replicationFactor": 1, "numShards": 1}}
        self.__request('PUT',
                       "collections/" + self.collection_name, body=collection_config)
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
        resp = self.__request('GET',
                              'collections/' + self.collection_name + "/stats")
        return json.loads(resp.data)

    def clear_collection(self):
        if self.stats()["documentCount"] > 0:
            resp = self.__request('POST',
                                  'solr/' + self.collection_name + '/update?commit=true',
                                  body={"delete": {"query": "*:*"}})

    def __query(self, qurl, handler="select", qparams=None):
        if not "wt" in qparams:
            qparams["wt"] = "json"
        resp = self.__request('GET', qurl + "/" + handler + '?' + urlencode(qparams))

        return json.loads(resp.data)

    def query(self, handler="select", pipeline="default", qparams=None, **__qp1):
        if qparams is None:
            qparams = {}
        qparams.update(__qp1)
        return self.__query(qurl='query-pipelines/%s/collections/%s' % (pipeline, self.collection_name),
                            handler=handler, qparams=qparams)

    def solrquery(self, handler="select", qparams=None, **__qp1):
        if qparams is None:
            qparams = {}
        qparams.update(__qp1)
        return self.__query(qurl='solr/%s' % self.collection_name, handler=handler, qparams=qparams)

    def commit(self):
        self.index({'commit': {}})

    def index(self, docs, pipeline="default"):
        resp = self.__request('POST', 'index-pipelines/%s/collections/%s/index' %
                              (pipeline, self.collection_name),
                              body=docs
                              )
        wrote = len(json.loads(resp.data))
        if wrote != len(docs):
            raise fusionpy.FusionError(resp,
                                       message="Submitted %d documents to index, but wrote %d" % (len(docs), wrote))

    def schema(self):
        resp = self.__request('GET', "solr/%s/schema" % self.collection_name)
        return json.loads(resp.data)["schema"]
