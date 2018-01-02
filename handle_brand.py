#!/usr/bin/env python
# coding:utf-8

"""
从接口获取每个品牌的logo URL，并存入配置文件。
"""

import requests

r = requests.get(url='https://cars.app.autohome.com.cn/xxxx')
print r.status_code
ret = r.json()

brand_list = ret['brandlist']

with open('./brand_logo.dat', 'w') as f:
    for i in xrange(len(brand_list)):
        b_list = brand_list[i]['list']
        for j in xrange(len(b_list)):
            brand_info = b_list[j]
            brand_id = brand_info['id']
            brand_img_url = brand_info['imgurl']
            str_line = "brand_logo_" + brand_id + " " + brand_img_url
            f.write(str_line + "\n")


print "Done."
