#!/usr/bin/python
# -*- coding: utf-8 -*-

class CkafkaExceptionBase(Exception):
    """
    @type code: int
    @param code: 错误类型

    @type message: string
    @param message: 错误描述
    
    @type data: dict
    @param data: 错误数据
    """
    def __init__(self, message, code=-1, data={}):
        self.code = code
        self.message = message
        self.data = data

    def get_info(self):
        return 'Code:%s, Message:%s, Data:%s\n' % (self.code, self.message, self.data)

    def __str__(self):
        return "CKAFKAExceptionBase  %s" % (self.get_info())

class CkafkaClientException(CkafkaExceptionBase):
    def __init__(self, message, code=-1, data={}):
        CkafkaExceptionBase.__init__(self, message, code, data)

    def __str__(self):
        return "CkafkaClientException  %s" % (self.get_info())
    
class CkafkaClientNetworkException(CkafkaClientException):
    """ 网络异常

        @note: 检查endpoint是否正确、本机网络是否正常等;
    """
    def __init__(self, message, code=-1, data={}):
        CkafkaClientException.__init__(self, message, code, data)

    def __str__(self):
        return "CkafkaClientNetworkException  %s" % (self.get_info())

class CkafkaClientParameterException(CkafkaClientException):
    """ 参数格式错误

        @note: 请根据提示修改对应参数;
    """
    def __init__(self, message, code=-1, data={}):
        CkafkaClientException.__init__(self, message, code, data)

    def __str__(self):
        return "CkafkaClientParameterException  %s" % (self.get_info())

class CkafkaServerNetworkException(CkafkaExceptionBase):
    """ 服务器网络异常
    """
    def __init__(self, status = 200, header = None, data = ""):
        if header == None:
            header = {}
        self.status = status
        self.header = header
        self.data = data

    def __str__(self):
        return "CkafkaServerNetworkException Status: %s\nHeader: %s\nData: %s\n" % \
            (self.status, "\n".join(["%s: %s" % (k,v) for k,v in self.header.items()]), self.data)

class CkafkaServerException(CkafkaExceptionBase):
    """ ckafka处理异常
    """
    def __init__(self, message, code=-1, data={}):
        CkafkaExceptionBase.__init__(self, message, code, data)

    def __str__(self):
        return "CkafkaServerException  %s" % (self.get_info())





