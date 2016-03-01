from nose.tools import *
import fusionpy
from unittest import TestCase
from stubserver import StubServer
from fusionpy.fusion import Fusion
from urlparse import urlparse
import json
import urllib3
import os

test_url = 'http://admin:topSecret5@localhost:8998/api/apollo/collections/phi'
test_path = os.path.dirname(os.path.realpath(__file__)) + "/"
http = urllib3.PoolManager(num_pools=1, maxsize=1, retries=0)
fa = {"fusion_url": test_url, "urllib3_pool_manager": http}


class FusionTest(TestCase):
    def setUp(self):
        self.server = StubServer(urlparse(test_url).port)
        self.server.run()

    def tearDown(self):
        self.server.stop()

    def test_ping_virgin(self):
        self.server.expect(method='GET', url='/api').and_return(mime_type="application/json",
                                                                file_content=test_path + "Fusion_ping_virgin_response.json")
        self.server.expect(method='GET', url='/api').and_return(mime_type="application/json",
                                                                file_content=test_path + "Fusion_ping_virgin_response.json")
        f = Fusion(**fa)
        self.assertFalse(f.ping())

    def test_ping_established(self):
        self.server.expect(method='GET', url='/api').and_return(mime_type="application/json",
                                                                file_content=test_path + "Fusion_ping_established_response.json")
        self.server.expect(method='GET', url='/api').and_return(mime_type="application/json",
                                                                file_content=test_path + "Fusion_ping_established_response.json")
        f = Fusion(**fa)
        self.assertTrue(f.ping())

    def test_set_admin_pw_bad_pw(self):
        #        HTTP/1.1 400 Bad Request
        #        {"code":"invalid-password"}
        self.server.expect(method='GET', url='/api').and_return(mime_type="application/json",
                                                                file_content=test_path + "Fusion_ping_virgin_response.json")
        self.server.expect(method='POST', url='/api', data=json.dumps({"password": "top_secret"})).and_return(
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
        self.server.expect(method='GET', url='/api').and_return(mime_type="application/json",
                                                                file_content=test_path + "Fusion_ping_established_response.json")
        self.server.expect(method='POST', url='/api', data=json.dumps({"password": "top_secret"})).and_return(
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
        self.server.expect(method='GET', url='/api').and_return(mime_type="application/json",
                                                                file_content=test_path + "Fusion_ping_virgin_response.json")
        self.server.expect(method='POST', url='/api', data=json.dumps({"password": "topSecret5"})).and_return(
            reply_code=201)
        self.server.expect(method='GET', url='/api').and_return(mime_type="application/json",
                                                                file_content=test_path + "Fusion_ping_established_response.json")
        f = Fusion(**fa)
        f.set_admin_password()
        f.ping()
