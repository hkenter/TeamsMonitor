# -*- coding: utf-8 -*-
# @File  : demo.py
# @Author: KellyCheng
# @Date  : 2020/7/31
# @Desc  :

import threading
import sys, signal
import pymsteams
from kafka import KafkaConsumer

KAFAKA_HOST = "ip:9092,ip:9092,ip:9092"  # 服务器端口地址
KAFAKA_PORT = 9092    # 端口号
KAFAKA_WARN_TOPIC = "error"  # topic
KAFAKA_INFO_TOPIC = "monitor"  #topic

class KafkaConsumerC():
    def __init__(self, kafkahost, kafkaport, kafkatopic, groupid, key):
        self.kafkaHost = kafkahost
        self.kafkaPort = kafkaport
        self.kafkatopic = kafkatopic
        self.groupid = groupid
        self.key = key
        self.consumer = KafkaConsumer(self.kafkatopic, group_id=self.groupid,
                                      bootstrap_servers='{kafka_host}:{kafka_port}'.format(
                                          kafka_host=self.kafkaHost,
                                          kafka_port=self.kafkaPort)
                                      )

    def consume_data(self):
        try:
            for message in self.consumer:
                yield message
        except KeyboardInterrupt as e:
            print(e)


class TeamsWebhookC:
    def __init__(self):
        webhook_url = "webhook_url"
        self.myTeamsMessage = pymsteams.connectorcard(webhook_url, http_proxy="http://ip:port/",
                                                 https_proxy="http://ip:port/")
        self.myTeamsMessage.title("Job异常提醒！")
        self.myTeamsMessage.text("请关注以下：")
        self.myTeamsMessage.addLinkButton("点我跳转002 WEB控制台",
                                     "http://")
        self.myTeamsMessage.addLinkButton("点我跳转001 WEB控制台",
                                     "http://")
        self.myTeamsMessage.color("#DC143C")

    def createWebhook(self, job_title, level, advice, reason, img_url):
        # create the section
        myMessageSection = pymsteams.cardsection()
        # Section Title
        myMessageSection.title(job_title)
        # Activity Elements
        myMessageSection.activityTitle(level)
        myMessageSection.activitySubtitle(advice)
        myMessageSection.activityImage(img_url)
        myMessageSection.activityText(reason)

        self.myTeamsMessage.addSection(myMessageSection)
        self.myTeamsMessage.printme()
        self.myTeamsMessage.send()

class JobInfoWebhookC:
    def __init__(self):
        webhook_url = "webhook_url"
        self.myTeamsMessage = pymsteams.connectorcard(webhook_url, http_proxy="http://ip:port/",
                                                      https_proxy="http://ip:port/")
        self.myTeamsMessage.title("Job 实时监控")
        self.myTeamsMessage.text("内容如下：")
        self.myTeamsMessage.color("#000080")

    def createWebhook(self, job_title, level, advice, reason):
        # create the section
        myMessageSection = pymsteams.cardsection()
        # Section Title
        myMessageSection.title(job_title)
        # Activity Elements
        myMessageSection.activityTitle(level)
        myMessageSection.activitySubtitle(advice)
        myMessageSection.activityImage("https://encrypted-tbn0.gstatic.com/images?q=tbn%3AANd9GcSor4Y5twpPy0VFBFkVYqcsjZIc1DPSPUhkuA&usqp=CAU")
        myMessageSection.activityText(reason)

        self.myTeamsMessage.addSection(myMessageSection)
        self.myTeamsMessage.printme()
        self.myTeamsMessage.send()


class RunInfoConsumerThread (threading.Thread):
    def __init__(self):
        super().__init__()

    def run(self):
        info_consumer = KafkaConsumerC(KAFAKA_HOST, KAFAKA_PORT, KAFAKA_INFO_TOPIC, "Teams", key=None)
        print("===========> consumer:", info_consumer)
        info_message = info_consumer.consume_data()
        for msg in info_message:
            print("INFO LEVEL------------------------------------------------------------------------------")
            print(msg.value)


class RunWarnConsumerThread(threading.Thread):
    def __init__(self):
        super().__init__()

    def run(self):
        warn_consumer = KafkaConsumerC(KAFAKA_HOST, KAFAKA_PORT, KAFAKA_WARN_TOPIC, "Teams", key=None)
        print("===========> consumer:", warn_consumer)
        warn_message = warn_consumer.consume_data()
        for msg in warn_message:
            print("WARN LEVEL------------------------------------------------------------------------------")
            print(msg.value)


def quit(signum, frame):
    print('You choose to stop me.')
    sys.exit()


if __name__ == '__main__':
    signal.signal(signal.SIGINT, quit)
    signal.signal(signal.SIGTERM, quit)

    thread1 = RunInfoConsumerThread()
    thread2 = RunWarnConsumerThread()
    # thread1.setDaemon(True)
    # thread2.setDaemon(True)

    thread1.start()
    thread2.start()
