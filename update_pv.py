#!/usr/bin/env python
# coding:utf8

"""
更新站内直播的 观众人数（直播状态）/关注人数（预告状态）
更新策略：
    每十分钟更新一次；
"""

import pymysql
import time
import urllib2
import json
import traceback


# 请求接口获取每个直播的PV
def request_api(live_id):
    api_url = "http://cms.api.autohome.com.cn/Wcf/LiveService.svc/LiveRoomPeopleLook?_appid=cms&id=" + str(live_id)
    req = urllib2.Request(api_url)
    f = urllib2.urlopen(req)
    rep = f.read()
    jrep = json.loads(rep)
    ret_pv = int(jrep["result"])
    return ret_pv


def get_zhibo_pv(live_pv):
    conn = pymysql.connect(host='192.168.224.24', port=3306, user='reader', passwd='reader',
                           db='search_data', charset='utf8')
    cursor = conn.cursor()
    # 获取所有有效的直播live_id
    sql = "SELECT live_id FROM zhibo WHERE ifnull(is_delete, 0) = 0"
    cursor.execute(sql)

    try:
        rows = cursor.fetchall()
    except Exception, ex:
        print Exception, ":", ex
        cursor.close()
        conn.close()
        retry_cnt = 0
        while retry_cnt < 3:
            try:
                conn = pymysql.connect(host='192.168.224.24', port=3306, user='reader', passwd='reader',
                                       db='search_data', charset='utf8')
            except Exception, ex:
                print Exception, ":", ex
                retry_cnt += 1
                time.sleep(1)
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
    if rows is not None and len(rows) > 0:
        for i in xrange(len(rows)):
            live_id = rows[i][0]
            live_pv[live_id] = request_api(live_id)

    cursor.close()
    conn.close()


# 推送给data_api接口
def push_to_data_api(all_live_pv):
    data = {}
    try:
        for k, v in all_live_pv.items():
            data['docid'] = int(k)
            inner_data = dict()
            inner_data['concernNum'] = int(v)
            data['data'] = json.dumps(inner_data)
            data['operation'] = 1
            # let worker log ignore the topic msg.
            data['log_ignore'] = 1
            jdata = json.dumps(data)

            req = urllib2.Request(url="http://192.168.224.30:9503/zhibo", data=jdata)
            f = urllib2.urlopen(req)
            rep = f.read()
            jrep = json.loads(rep)
            if jrep['returncode'] != 0:
                print "Error occured", jrep['message']
    except Exception, e:
        print e
        traceback.print_exc()


if __name__ == "__main__":
    all_live_pv_r = {}
    print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())) + " : " + 'Start to get live PV...'
    get_zhibo_pv(all_live_pv_r)
    print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())) + " : " + 'Get all live PV done.'
    
    for key, value in all_live_pv_r.items():
        print key, value
   
    print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())) + " : " + 'Start to update PV...'
    push_to_data_api(all_live_pv_r)
    print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())) + " : " + 'Update PV done.'
