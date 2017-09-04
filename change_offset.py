#!/usr/bin/env python
#coding=utf-8

import sys,json
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append('lib')

from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print '功能: 移动consumer的消费指针至指定时间处'
        print 'Kafka Server为: 192.168.199.5:9092 等同15/25:9092'
        print 'Usage: .py [topic] [group] [date]'
    else:
        topic = sys.argv[1]
        group = sys.argv[2]
        date = sys.argv[3]
        server = '192.168.199.5:9092'
        print '将%s的%s的使用者%s的时间轴调整至%s...'%(server, topic, group, date)
        client = KafkaClient(server)
        consumer = SimpleConsumer(client, group, topic)
        step = 10000
        consumer.seek(step, 0)
        cnt = 0
        while step > 1:
            cnt = cnt + 1
            message = consumer.get_message()
            msg = json.loads(message.message.value)
            if msg.has_key('up_time'):
                if cnt%2 == 0: 
                    print 'Processed %s to date %s'%(cnt, msg['up_time'])
                if msg['up_time'] > date:
                    step = int(step*2/3)
                    consumer.seek(-step, 1)
                elif msg['up_time'] == date: break
                else:
                    consumer.seek(step, 1)
            else: break
