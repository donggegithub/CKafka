#!/usr/bin/python
# -*- coding: utf-8 -*-


from ckafka.ckafka_client import CkafkaClient


class Account:
    """
    Account类对象不是线程安全的，如果多线程使用，需要每个线程单独初始化Account类对象
    """
    def __init__(self,secretId, secretKey,region,host='https://ckafka.api.qcloud.com'):
        """
            @type host: string
            @param host: 访问的url，例如：https://ckafka.api.qcloud.com

            @type secretId: string
            @param secretId: 用户的secretId, 腾讯云官网获取

            @type secretKey: string
            @param secretKey: 用户的secretKey，腾讯云官网获取

            @note: Exception
            :: ckafkaClientParameterException host格式错误
        """
        self.secretId = secretId
        self.secretKey = secretKey
        self.ckafka_client = CkafkaClient(host, secretId, secretKey,region)

    def set_sign(self, sign='sha256'):
        '''
          @fucntion set_sign : set sign method
          @sign  sha256 or sha1
        '''
        self.ckafka_client.set_sign_method(sign)

    def list_instance(self,):
        """
        """
        params = {}
        ret_pkg = self.ckafka_client.list_instance(params)
        return (ret_pkg['data']['totalCount'],ret_pkg['data']['instanceList'])

    def create_topic(self,instanceId, topicName, partitionNum, replicationNum):
        params={}
        if instanceId:
            params['instanceId'] = instanceId
        if topicName:
            params['topicName'] = topicName
        if partitionNum >0:
            params['partitionNum'] = partitionNum
        if replicationNum > 0:
            params['replicaNum'] = replicationNum

        ret_pkg = self.ckafka_client.create_topic(params)

    def copy_topic(self,instanceId, topic):
        params = {}
        params['instanceId']=instanceId

        params['topicName'] = topic['topicName']
        params['note'] = topic['note']
        params['partitionNum'] = topic['partitionNum']
        params['replicaNum'] = topic['replicaNum']
        if topic['enableWhiteList'] == 1:
            params['enableWhiteList'] = 1 
            index = 0
            for ip in topic['ipWhiteList']:
                params['ipWhiteList.' + str(index) ] = ip
                index=index+1
        configs = topic['config']
        if 'min.insync.replicas' in configs.keys():
            params['minInsyncReplicas'] = configs['min.insync.replicas']
        if 'unclean.leader.election.enable'  in configs.keys():
            params['uncleanLeaderElectionEnable'] = 1 if  configs['unclean.leader.election.enable'] == 'true' else 0
        if 'retention.ms' in configs.keys():
            params['retentionMs'] = configs['retention.ms']
        if 'cleanup.policy' in configs.keys():
            params['cleanUpPolicy'] = configs['cleanup.policy']
        if 'segment.ms' in configs.keys():
            params['segmentMs'] = configs['segment.ms']

        ret_pkg = self.ckafka_client.create_topic(params)


    def list_topic(self, instanceId, searchWord="", limit=-1, offset=""):

        """ 列出Account的主题

            @type searchWord: string
            @param searchWord: 主题名的前缀

            @type limit: int
            @param limit: list_topic最多返回的主题数

            @type offset: string
            @param offset: list_topic的起始位置，上次list_topic返回的next_offset

            @rtype: tuple
            @return: topicURL的列表和下次list topic的起始位置; 如果所有topic都list出来，next_offset为"".
        """
        params = {}
        params["instanceId"] = instanceId

        if searchWord != "":
            params['searchWord'] = searchWord
        if limit != -1:
            params['limit'] = limit
        if offset != "":
            params['offset'] = offset
        ret_pkg = self.ckafka_client.list_topic(params)

        if offset == "":
            next_offset = len(ret_pkg['topicList'])
        else:
            next_offset = offset + len(ret_pkg['topicList'])

        return (ret_pkg['totalCount'], ret_pkg['topicList'], next_offset)


    def get_topic_attributes(self,instanceId, topicName):
        params={
        "instanceId":instanceId,
        "topicName":topicName
        }

        ret_pkg = self.ckafka_client.get_topic_attributes(params)
        return ret_pkg

    def list_topic_detail(self, instanceId, searchWord="", limit=-1, offset=""):

        """ 列出Account的主题

            @type searchWord: string
            @param searchWord: 主题名的前缀

            @type limit: int
            @param limit: list_topic最多返回的主题数

            @type offset: string
            @param offset: list_topic的起始位置，上次list_topic返回的next_offset

            @rtype: tuple
            @return: topicURL的列表和下次list topic的起始位置; 如果所有topic都list出来，next_offset为"".
        """
        params = {}
        params["instanceId"] = instanceId

        if searchWord != "":
            params['searchWord'] = searchWord
        if limit != -1:
            params['limit'] = limit
        if offset != "":
            params['offset'] = offset
        ret_pkg = self.ckafka_client.list_topic_detail(params)

        if offset == "":
            next_offset = len(ret_pkg['topicList'])
        else:
            next_offset = offset + len(ret_pkg['topicList'])

        return (ret_pkg['totalCount'], ret_pkg['topicList'], next_offset)






