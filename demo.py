# -*- coding: utf-8 -*-
# @File  : demo.py
# @Author: KellyCheng
# @Date  : 2020/7/31
# @Desc  :

import threading
import sys, signal
import pymsteams
from kafka import KafkaConsumer

KAFAKA_HOST = "172.17.0.8:9092,172.17.0.2:9092,172.17.0.5:9092"  # 服务器端口地址
KAFAKA_PORT = 9092    # 端口号
KAFAKA_WARN_TOPIC = "job-error"  # topic
KAFAKA_INFO_TOPIC = "job-monitor"  #topic

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
        self.myTeamsMessage.addLinkButton("点我跳转cndcwprdetlp002 WEB控制台",
                                     "http://cndcwprdetlp002:8080/DataServices/launch/logon.action")
        self.myTeamsMessage.addLinkButton("点我跳转cndcwetlp001 WEB控制台",
                                     "http://cndcwetlp001:8080/DataServices/launch/logon.do")
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
            msgSplit = bytes.decode(msg.value).split(",")
            level = "Info"
            advice = ""
            if msgSplit[4] == 'S':
                advice = "正在运行"
            elif msgSplit[4] == "D":
                advice = "运行完毕"
            else:
                advice = "发送异常"
            run_time = msgSplit[5] + " => " + msgSplit[6]
            reason = "执行时间：" + run_time
            jobInfoWebhookC = JobInfoWebhookC()
            jobInfoWebhookC.createWebhook(msgSplit[3], level, advice, reason)


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
            msgSplit = bytes.decode(msg.value).split(",")
            level = "Info"
            advice = ""
            reason = ""
            run_time = msgSplit[5] + " => " + msgSplit[6]
            plan_time = msgSplit[12] + " => " + msgSplit[13]
            if msgSplit[msgSplit.__len__() - 3] == "1" and msgSplit[msgSplit.__len__() - 2] == "0":
                level = "Warning"
                advice = "建议：可能需要人工介入"
                reason = "原因：非计划时间内执行<br/>执行时间：" + run_time + "<br/>计划时间：" + plan_time
                teamsWebHook = TeamsWebhookC()
                teamsWebHook.createWebhook(msgSplit[3], level, advice, reason, "https://www.socialworker.com/downloads/296/download/caution-152926_640.png?cb=ca1ea16078f1b6307db06ea9cc2d26ff")
            elif msgSplit[msgSplit.__len__() - 2] == "1":
                level = "Error"
                advice = "建议：请立即人工介入处理"
                reason = "原因：运行失败<br/>执行时间：" + run_time + "<br/>计划时间：" + plan_time
                teamsWebHook = TeamsWebhookC()
                teamsWebHook.createWebhook(msgSplit[3], level, advice, reason, "https://wusfnews.wusf.usf.edu/sites/wusf/files/201206/error2.png")


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
