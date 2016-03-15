from nose.tools import *
import fusionpy
from unittest import TestCase
from stubserver import StubServer
from fusionpy.fusion import Fusion
import fusionpy.fusioncollection
from urlparse import urlparse
import json
import urllib3
import os
from fusionpy.connectors import HttpFusionRequester

test_url = 'http://admin:topSecret5@localhost:8998/api/apollo/collections/phi'
test_path = os.path.dirname(os.path.realpath(__file__)) + "/"
http = urllib3.PoolManager(num_pools=1, maxsize=1, retries=0)
fa = {"fusion_url": test_url, "requester": HttpFusionRequester(test_url, urllib3_pool_manager=http)}

class FusionTest(TestCase):
    def setUp(self):
        self.server = StubServer(urlparse(test_url).port)
        self.server.run()

    def tearDown(self):
        self.server.stop()

    def test_ping_virgin(self):
        self.server.expect(method='GET', url='/api$').and_return(mime_type="application/json",
                                                                 file_content=test_path + "Fusion_ping_virgin_response.json")
        self.server.expect(method='GET', url='/api$').and_return(mime_type="application/json",
                                                                 file_content=test_path + "Fusion_ping_virgin_response.json")
        f = Fusion(**fa)
        self.assertFalse(f.ping())

    def test_ping_established(self):
        self.server.expect(method='GET', url='/api$').and_return(mime_type="application/json",
                                                                 file_content=test_path + "Fusion_ping_established_response.json")
        self.server.expect(method='GET', url='/api$').and_return(mime_type="application/json",
                                                                 file_content=test_path + "Fusion_ping_established_response.json")
        f = Fusion(**fa)
        self.assertTrue(f.ping())

    def test_set_admin_pw_bad_pw(self):
        #        HTTP/1.1 400 Bad Request
        #        {"code":"invalid-password"}
        self.server.expect(method='GET', url='/api$').and_return(mime_type="application/json",
                                                                 file_content=test_path + "Fusion_ping_virgin_response.json")
        self.server.expect(method='POST', url='/api$', data=json.dumps({"password": "top_secret"})).and_return(
            reply_code=400,
            content='{"code":"invalid-password"}')

        f = Fusion(**fa)
        try:
            f.set_admin_password("top_secret")
            self.fail("Should have had an exception")
        except fusionpy.FusionError as fe:
            self.assertTrue(fe.response.status == 400)

    def test_Set_admin_pw_again(self):
        #          HTTP/1.1 409 Conflict
        self.server.expect(method='GET', url='/api$').and_return(mime_type="application/json",
                                                                 file_content=test_path + "Fusion_ping_established_response.json")
        self.server.expect(method='POST', url='/api$', data=json.dumps({"password": "top_secret"})).and_return(
            reply_code=409)

        f = Fusion(**fa)
        try:
            f.set_admin_password()
            self.fail("Should have had an exception")
        except fusionpy.FusionError as fe:
            self.assertEqual(409, fe.response.status)

    def test_set_admin_pw(self):
        #        HTTP/1.1 201 Created
        #        (no content)
        self.server.expect(method='GET', url='/api$').and_return(mime_type="application/json",
                                                                 file_content=test_path + "Fusion_ping_virgin_response.json")
        self.server.expect(method='POST', url='/api$', data=json.dumps({"password": "topSecret5"})).and_return(
            reply_code=201)
        self.server.expect(method='GET', url='/api$').and_return(mime_type="application/json",
                                                                 file_content=test_path + "Fusion_ping_established_response.json")
        f = Fusion(**fa)
        f.set_admin_password()
        f.ping()

    def test_get_index_pipelines(self):
        self.server.expect(method='GET', url='/api$'). \
            and_return(mime_type="application/json",
                       file_content=test_path + "Fusion_ping_established_response.json")
        for i in range(0, 2):
            self.server.expect(method='GET', url='/api/apollo/index-pipelines$'). \
                and_return(mime_type="application/json",
                           file_content=test_path + "some_index_pipelines.json")
        f = Fusion(**fa)
        pipelines = f.get_index_pipelines()

        # This reference pipeline file may change, so this test could assert that certain pipelines are present
        # and even go so far as to dissect them, but this is not a test of json.loads().
        self.assertTrue(len(pipelines) > 8)
        pmap = {}
        for p in pipelines:
            pmap[p['id']] = p['stages']

        self.assertEqual(len(pipelines), len(pmap))

        allpipelines = f.get_index_pipelines(include_system=True)

        self.assertTrue(len(allpipelines) > len(pipelines))

    def test_get_query_pipelines(self):
        self.server.expect(method='GET', url='/api$'). \
            and_return(mime_type="application/json",
                       file_content=test_path + "Fusion_ping_established_response.json")
        for i in range(0, 2):
            self.server.expect(method='GET', url='/api/apollo/query-pipelines$'). \
                and_return(mime_type="application/json",
                           file_content=test_path + "some_query_pipelines.json")
        f = Fusion(**fa)
        pipelines = f.get_query_pipelines()

        # This reference pipeline file may change, so this test could assert that certain pipelines are present
        # and even go so far as to dissect them, but this is not a test of json.loads().
        self.assertTrue(len(pipelines) > 7, len(pipelines))
        pmap = {}
        for p in pipelines:
            pmap[p['id']] = p['stages']

        self.assertEqual(len(pipelines), len(pmap))

        allpipelines = f.get_query_pipelines(include_system=True)

        self.assertTrue(len(allpipelines) > len(pipelines))

    def test_get_collection_stats(self):
        self.server.expect(method='GET', url='/api$'). \
            and_return(mime_type="application/json",
                       file_content=test_path + "Fusion_ping_established_response.json")
        self.server.expect(method='GET', url='/api/apollo/collections/phi/stats$').and_return(
            file_content=test_path + "phi-stats.json")
        with open(test_path + "phi-stats.json") as f:
            json_stats = json.loads(f.read())

        self.assertEquals(json_stats, Fusion(**fa).get_collection().stats())

    def test_create_query_pipeline(self):
        self.server.expect(method='GET', url='/api$'). \
            and_return(mime_type="application/json",
                       file_content=test_path + "Fusion_ping_established_response.json")
        self.server.expect(method='POST', url='/api/apollo/query-pipelines/$').and_return(
            file_content=test_path + "create-pipeline-response.json")

        with open(test_path + "create-pipeline-response.json") as f:
            qp = json.loads(f.read())

        Fusion(**fa).add_query_pipeline(qp)


class NoNetworkTest(TestCase):
    """
    These tests don't require a network mock
    """

    def test_create_field(self):
        field_definition = {
            "name": "label",
            "type": "shingleString",
            "indexed": True,
            "stored": True
        }

        class MockCollection:
            def __init__(self, tc):
                self.tc = tc

            def request(self, method, path, headers=None, fields=None, body=None, validate=None):
                self.tc.assertEquals({'add-field': field_definition}, body)

        mc = MockCollection(self)

        fusionpy.fusioncollection.Fields(mc).add(field_definition)

    def test_create_field_type(self):
        field_type_definition = {
            "name": "shingleString",
            "class": "solr.TextField",
            "positionIncrementGap": "100",
            "analyzer": {
                "charFilters": [
                    {
                        "class": "solr.PatternReplaceCharFilterFactory",
                        "replacement": "$1$1",
                        "pattern": "([a-zA-Z])\\\\1+"
                    }
                ],
                "tokenizer": {
                    "class": "solr.WhitespaceTokenizerFactory"
                },
                "filters": [
                    {
                        "class": "solr.WordDelimiterFilterFactory",
                        "preserveOriginal": "0"
                    },
                    {
                        "class": "solr.ShingleFilterFactory",
                        "minShingleSize": "2",
                        "maxShingleSize": "5",
                        "outputUnigrams": "true"
                    }
                ]
            }
        }

        class MockCollection:
            def __init__(self, tc):
                self.tc = tc

            def request(self, method, path, headers=None, fields=None, body=None, validate=None):
                self.tc.assertEquals({'add-field-type': field_type_definition}, body)

        mc = MockCollection(self)

        fusionpy.fusioncollection.FieldTypes(mc).add(field_type_definition)
