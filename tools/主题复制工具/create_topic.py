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
@kafka
@ topicCmd:kafka-topics.sh脚本路径，可以填相对，需要配置PATH
@ zk :zookeeper 地址

@ckafka
@secretId， secretKey  这里可以查询 secretId 和secretKeyhttps://console.cloud.tencent.com/cam/capi

@region : 填写购买的ckafka地域信息，例如 北京： bj , 上海：sh, 广州：gz
@instanceId:填写购买的ckafkaId信息，可以在控制台查看 https://console.cloud.tencent.com/ckafka?rid=8

@依赖：python2 ，zookeeper, kafka-topics.sh脚本, 访问ckafka 默认使用公网域名，如使用内网可以修改 ckafka/account 构造函数host默认参数

@注意：
	1 脚本非线程安全
	2 同步topic过程中，不会开启ckafka 白名单功能。
	3 创建失败会抛出异常信息， 创建成功会打印创建成功的topic信息。，无显示信息说明zookeeper脚本执行失败，会有异常抛出


@author:york
@date:2018-7-17
'''


def create_topic():
    topicCmd='/media/york/kafka_2.9.1-0.8.2.0/bin/kafka-topics.sh'
    zk ='localhost:2181'

    secretId = ''
    secretKey = ''
    instanceId=''
    region='bj'
    my_account =Account(secretId, secretKey, region)
    status , output = listTopicInfo(topicCmd, zk)
    for item in output.split('\n'):
        topicName, partition, replication = splitTopicInfo(item)
        try:
            my_account.create_topic(instanceId, topicName, partition,replication)
            print "topicname %s , partition %d, replication %d  created \n" % (topicName,partition, replication)
        except CkafkaExceptionBase,e:
            print  e

if __name__ == '__main__':
    create_topic()
