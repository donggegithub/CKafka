#!/Usr/bin/env python
#coding=utf8
import sys
import os
import commands
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/")

from ckafka.account import Account
from ckafka.ckafka_exception import CkafkaExceptionBase
from ckafka.ckafka_tool import splitTopicInfo, listTopicInfo

'''
@ckafka
@secretId， secretKey  这里可以查询 secretId 和secretKey https://console.cloud.tencent.com/cam/capi

@region : 填写购买的ckafka地域信息，例如 北京： bj , 上海：sh, 广州：gz
@instanceId:填写购买的ckafkaId信息，可以在控制台查看 https://console.cloud.tencent.com/ckafka?rid=8

@依赖：python2 ，zookeeper, kafka-topics.sh脚本, 访问ckafka 默认使用公网域名，如使用内网可以修改 ckafka/account 构造函数host默认参数

@注意：
	1 脚本非线程安全
	2 同步topic过程中，不会开启ckafka 白名单功能。
	3 创建失败会抛出异常信息。


@author:york
@date:2018-7-17
'''
def copy_topic():
    secretId=''
    secretKey=''
    dstInstanceId=''
    srcInstanceId=''
    region=''

    my_account=Account(secretId,secretKey,region)

    offset = 0
    next = 0 
    limit =2

    while True:
        totalCount ,topicList,next = my_account.list_topic_detail(srcInstanceId,"",limit,offset)
        print totalCount
        print topicList
        print next

        for topic in topicList:

            try:
                topicMeta = my_account.get_topic_attributes(srcInstanceId,topic['topicName'])
                topicMeta['topicName']=topic['topicName']
                topicMeta['replicaNum'] = topic['replicaNum']
                my_account.copy_topic(dstInstanceId,topicMeta)
            except CkafkaExceptionBase ,e:
                print e 
        if next >= totalCount:
            break
        offset = next

if __name__ == '__main__':
    copy_topic()
