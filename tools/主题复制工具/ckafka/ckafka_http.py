#!/usr/bin/python
# -*- coding: utf-8 -*-


import socket
from httplib import HTTPConnection, BadStatusLine, HTTPSConnection
from ckafka.ckafka_exception import CkafkaClientNetworkException

class CkafkaHTTPConnection(HTTPConnection):
    def __init__(self, host, port=None, strict=None):
        HTTPConnection.__init__(self, host, port, strict)
        self.request_length = 0

    def send(self, astr):
        HTTPConnection.send(self, astr)
        self.request_length += len(astr)

    def request(self, method, url, body=None, headers={}):
        self.request_length = 0
        HTTPConnection.request(self, method, url, body, headers)

class CkafkaHTTPSConnection(HTTPSConnection):
    def __init__(self, host, port=None, strict=None):
        HTTPSConnection.__init__(self, host, port, strict=strict)
        self.request_length = 0

    def send(self, astr):
        HTTPSConnection.send(self, astr)
        self.request_length += len(astr)

    def request(self, method, url, body=None, headers={}):
        self.request_length = 0
        HTTPSConnection.request(self, method, url, body, headers)

class CkafkaHttp:
    def __init__(self, host, connection_timeout = 10, keep_alive = True, is_https=False):
        if is_https:
            self.conn = CkafkaHTTPSConnection(host)
        else:
            self.conn = CkafkaHTTPConnection(host)
        self.connection_timeout = connection_timeout
        self.keep_alive = keep_alive
        self.request_size = 0
        self.response_size = 0

    def set_connection_timeout(self, connection_timeout):
        self.connection_timeout = connection_timeout

    def set_keep_alive(self, keep_alive):
        self.keep_alive = keep_alive

    def is_keep_alive(self):
        return self.keep_alive

    def send_request(self, req_inter):
        try:
            if req_inter.method == 'GET':
                req_inter_url = '%s?%s' % (req_inter.uri, req_inter.data)
                self.conn.request(req_inter.method, req_inter_url, None, req_inter.header)
            elif req_inter.method == 'POST':
                self.conn.request(req_inter.method, req_inter.uri, req_inter.data, req_inter.header)
            else:
                raise Exception, 'Method only support (GET, POST)'
            self.conn.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            try:
                http_resp = self.conn.getresponse()
            except BadStatusLine:
                #open another connection when keep-alive timeout
                self.conn.close()
                self.conn.request(req_inter.method, req_inter.uri, req_inter.data, req_inter.header)
                self.conn.sock.settimeout(self.connection_timeout+int(UserTimeOut))
                self.conn.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                http_resp = self.conn.getresponse()
            headers = dict(http_resp.getheaders())
            resp_inter = ResponseInternal(status = http_resp.status, header = headers, data = http_resp.read())
            self.request_size = self.conn.request_length
            self.response_size = len(resp_inter.data)
            if not self.is_keep_alive():
                self.conn.close()
            return resp_inter
        except Exception,e:
            self.conn.close()
            raise CkafkaClientNetworkException(str(e))

class RequestInternal:
    def __init__(self, method = "", uri = "", header = None, data = ""):
        if header == None:
            header = {}
        self.method = method
        self.uri = uri
        self.header = header
        self.data = data

    def __str__(self):
        return "Method: %s\nUri: %s\nHeader: %s\nData: %s\n" % \
                (self.method, self.uri, "\n".join(["%s: %s" % (k,v) for k,v in self.header.items()]), self.data)

class ResponseInternal:
    def __init__(self, status = 0, header = None, data = ""):
        if header == None:
            header = {}
        self.status = status
        self.header = header
        self.data = data

    def __str__(self):
        return "Status: %s\nHeader: %s\nData: %s\n" % \
            (self.status, "\n".join(["%s: %s" % (k,v) for k,v in self.header.items()]), self.data)
