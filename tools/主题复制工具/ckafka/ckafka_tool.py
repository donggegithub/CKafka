#!/usr/bin/python 
# -*- coding: utf-8 -*-
import os
import sys
import commands
def splitTopicInfo(topicInfo):
    name = ''
    partition = 0
    replication = 0
    for item in topicInfo.split('\t'):
        if item:
            info = item.split(':')
            if info[0].find('Topic') == 0:
                name = info[1]
            if info[0].find('Par') == 0:
                partition = int(info[1])
            if info[0].find('Re') == 0:
                replication = int(info[1])
    return name,partition,replication


def listTopicInfo(cmd,zk):
    path = os.path.dirname(os.path.abspath(__file__))
    (status, output) = commands.getstatusoutput('chmod +x ' + path + '/listtopic')
    (status,output) = commands.getstatusoutput('bash ' + path +'/listtopic ' + cmd + ' ' + zk)
    return (status,output)
