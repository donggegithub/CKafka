#!/usr/bin/python
# -*- coding: utf-8 -*-

import urllib
import copy
import random
from ckafka.ckafka_exception import *
from ckafka.ckafka_http import CkafkaHttp, RequestInternal
from sign import Sign
import json
import time
import sys

URISEC = '/v2/index.php'

class CkafkaClient:
    def __init__(self, host, secretId, secretKey, region,version="SDK_Python_1.3",):
        self.host, self.is_https = self.process_host(host)
        self.secretId = secretId
        self.secretKey = secretKey
        self.version = version
        self.region = region
        self.http = CkafkaHttp(self.host, is_https=self.is_https)
        self.sign_method = 'sha1'
        self.method = 'GET'

    def set_method(self, method='POST'):
        """
        method: POST OR GET
        """
        self.method = method.upper()
    def set_sign_method(self, sign_method='sha1'):
        '''
        @function : set sign method , and current support sha1 and sha256 method 
        @sign_method :  sign method support sha1 and sha256
        @return none or exception for sign method 
        '''
        if sign_method == 'sha1' or sign_method == 'sha256':
            self.sign_method = sign_method
        else:
            raise CkafkaClientParameterException('Only support sha1 or sha256. invalid method:%s' % sign_method)
        

    def set_connection_timeout(self, connection_timeout):
        self.http.set_connection_timeout(connection_timeout)

    def set_keep_alive(self, keep_alive):
        self.http.set_keep_alive(keep_alive)

    def close_connection(self):
        self.http.conn.close()

    def process_host(self, host):
        if host.startswith("http://"):
            if host.endswith("/"):
                host = host[:-1]
            host = host[len("http://"):]
            return host, False
        elif host.startswith("https://"):
            if host.endswith("/"):
                host = host[:-1]
            host = host[len("https://"):]
            return host, True
        else:
            raise CKAFKAClientParameterException("Only support http(s) prototol. Invalid host:%s" % host)

    def build_req_inter(self, action, params, req_inter):
        _params = copy.deepcopy(params)
        _params['Action'] = action[0].upper() + action[1:]
        _params['RequestClient'] = self.version

        if (_params.has_key('SecretId') != True):
            _params['SecretId'] = self.secretId

        if (_params.has_key('Nonce') != True):
            _params['Nonce'] = random.randint(1, sys.maxint)

        if (_params.has_key('Timestamp') != True):
            _params['Timestamp'] = int(time.time())

        if(_params.has_key('Region') != True):
            _params['Region'] = self.region            
        if (_params.has_key('SignatureMethod') != True):
            if self.sign_method == 'sha256':
                _params['SignatureMethod'] = 'HmacSHA256'
            else:
                _params['SignatureMethod'] = 'HmacSHA1'
                
        sign = Sign(self.secretId, self.secretKey)
        _params['Signature'] = sign.make(self.host, req_inter.uri, _params, req_inter.method, self.sign_method)

        req_inter.data = urllib.urlencode(_params)

        self.build_header(req_inter)

    def build_header(self, req_inter):
        req_inter.header["Host"] = self.host
        if self.http.is_keep_alive():
            req_inter.header["Connection"] = "Keep-Alive"

    def check_status(self, resp_inter):
        if resp_inter.status != 200:
            raise CKAFKAServerNetworkException(resp_inter.status, resp_inter.header, resp_inter.data)
        resp = json.loads(resp_inter.data)
        code, message = resp['code'], resp['message']

        if code != 0:
            raise CkafkaServerException(message=message, code=code, data=resp)

    def request(self, action, params):
        # make request internal
        req_inter = RequestInternal(self.method, URISEC)
        self.build_req_inter(action, params, req_inter)
        resp_inter = self.http.send_request(req_inter)
        return resp_inter

#=============================================== operation===============================================#

    def create_topic(self, params):
        resp_inter = self.request('CreateTopic', params)
        self.check_status(resp_inter)
        ret = json.loads(resp_inter.data)
        return ret
  
    def list_instance(self, params):
        resp_inter = self.request('ListInstance',params)
        self.check_status(resp_inter)
        ret = json.loads(resp_inter.data)
        return ret

    def list_topic(self, params):
        resp_inter = self.request('ListTopic', params)
        self.check_status(resp_inter)
        ret = json.loads(resp_inter.data)
        return ret['data']
    def list_topic_detail(self,params):
        resp_inter = self.request('ListTopicDetail',params)
        self.check_status(resp_inter)
        ret = json.loads(resp_inter.data)
        return ret['data']
    def get_topic_attributes(self,params):
        resp_inter = self.request('GetTopicAttributes',params)
        self.check_status(resp_inter)
        ret = json.loads(resp_inter.data)
        return ret['data']


 
