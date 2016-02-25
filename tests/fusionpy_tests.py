from nose.tools import *
import fusionpy
import unittest
from unittest import TestCase
from stubserver import StubServer
from fusionpy.fusion import Fusion
from urlparse import urlparse
import json

test_url = 'http://admin:topSecret5@localhost:8998/api/apollo/collections/phi'


class FusionTest(TestCase):
    def setUp(self):
        self.server = StubServer(urlparse(test_url).port)
        self.server.run()

    def tearDown(self):
        self.server.stop()

    def test_ping_virgin(self):
        self.server.expect(method='GET', url='/api').and_return(mime_type="application/json",
                                                                file_content="./tests/Fusion_ping_virgin_response.json")
        self.server.expect(method='GET', url='/api').and_return(mime_type="application/json",
                                                                file_content="./tests/Fusion_ping_virgin_response.json")
        f = Fusion(fusion_url=test_url)
        self.assertFalse(f.ping())

    def test_ping_established(self):
        self.server.expect(method='GET', url='/api').and_return(mime_type="application/json",
                                                                file_content="./tests/Fusion_ping_established_response.json")
        self.server.expect(method='GET', url='/api').and_return(mime_type="application/json",
                                                                file_content="./tests/Fusion_ping_established_response.json")
        f = Fusion(fusion_url=test_url)
        self.assertTrue(f.ping())

#    def test_set_admin_pw_bad_pw(self):
#         HTTP/1.1 400 Bad Request
#         {"code":"invalid-password"}
#        self.server.expect(method='POST', url='/api', data=json.dumps({"password": "top_secret"}))

#    def test_Set_admin_pw_again(self):
#           HTTP/1.1 409 Conflict

#    def test_set_admin_pw(self):
#        HTTP/1.1 201 Created
#        (no content)
#        self.server.expect(method='POST', url='/api', data=json.dumps({"password": "topSecret5"}))
