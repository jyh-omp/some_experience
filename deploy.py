#!/usr/bin/env python
# encoding:utf8

"""
自动化部署脚本
功能1：部署新版本文件，同时若目标文件夹存在同名文件则进行备份；
功能2：回滚到上一个版本
功能3：reload新发布的程序
"""

import os
from hashlib import md5
import ConfigParser
import subprocess
import commands


# 大文件生成MD5值（分块读取）
def calculate_md5_for_big_file(big_file):
    m = md5()
    chunk_size = 8192
    with open(big_file, 'r') as f:
        while 1:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            m.update(chunk)
    return m.hexdigest()


# 生成文件MD5值
def calculate_md5_for_file(conf_file):
    stat_info = os.stat(conf_file)
    if int(stat_info.st_size)/(1024*1024) >= 100:
        return calculate_md5_for_big_file(conf_file)

    m = md5()
    with open(conf_file, 'r') as f:
        m.update(f.read())
    return m.hexdigest()


# 解析配置文件
def parse_config(conf_file):
    parser = ConfigParser.ConfigParser()
    parser.readfp(open(conf_file))
    sections = parser.sections()

    # 配置文件的解析最终结果为一个dict
    config = {}
    for sec in sections:
        if sec == 'main':
            config['operation'] = parser.get('main', 'operation')
            config['main_path'] = parser.get('main', 'main_path')
            config['cache_path'] = parser.get('main', 'cache_path')
            continue

        tmp = []
        for item in parser.items(sec):
            tmp.append(item[1].split(','))
        config[sec] = tmp

    print 'config-->', config
    return config


# reload文件操作
def reload_operation(url, target_ip, is_merge):
    r_url = url.strip()
    if len(r_url):
        if is_merge == 0:
            suffix = "_reload_module=key_ranking"
        else:
            suffix = "_control=1&reload_mod=mix_module"
        r_url += suffix
    else:
        print 'Reload url is empty, target_ip:', target_ip

    cmd = "wget -cq '" + r_url + "'" + " -O wget." + target_ip
    print cmd
    subprocess.call(cmd, shell=True)
    # 查看是否reload成功，如果成功，则会生成文件名'wget.xxxxxx'的log文件
    cmd = 'find . -name ' + 'wget.' + target_ip
    print cmd
    status, out_list = commands.getstatusoutput(cmd)
    if status != 0 or len(out_list) == 0:
        print 'reload failed, target_ip:', target_ip
    ssh_cmd = 'ssh -p 30000 root@' + target_ip + ' ' + "pgrep 'Searchear'"
    print ssh_cmd
    status, output = commands.getstatusoutput(ssh_cmd)
    out_list = output.split('\n')
    if status != 0 or len(out_list) == 0:
        print 'pgrep got error, target_ip:', target_ip


def main(config_dict):
    op = config_dict.pop('operation')
    main_path = config_dict.pop('main_path')
    cache_path = config_dict.pop('cache_path')

    if op not in ['update', 'rollback', 'reload']:
        print 'Operation config error.'
        return

    for f, target_machines in config_dict.items():  # 依次处理每个要更新的文件
        f_path = main_path + '/' + f
        c_path = cache_path + '/' + f
        cb_path = c_path + '.back'
        e = os.path.exists(f_path)
        me = os.path.exists(c_path)
        meb = os.path.exists(cb_path)

        # t_m是含有两个元素的列表，第一个元素为目标路径，第二个元素为reload所需的URL路径
        for t_m in target_machines:  # 依次处理每个目标机器
            target = t_m[0]
            if len(t_m) > 1:
                target_url = t_m[1]  # reload所需的url
            else:
                target_url = ''
            target_ip = ''
            target_path = ''
            try:
                target_ip, target_path = target.split(':')
            except Exception, e:
                print Exception, ':', e
            # 目标机器上目标文件的绝对路径+名称
            r_path = target_path + '/' + f
            if op == 'update':  # 更新操作
                if not e or not me or calculate_md5_for_file(f_path) != calculate_md5_for_file(c_path):
                    # 首先备份原版本文件
                    # cmd = 'ssh -p 30000 root@' + target_ip + ' cp ' + r_path + ' ' + r_path + '.back'
                    cmd = 'ssh jiangyuanheng@' + target_ip + ' cp ' + r_path + ' ' + r_path + '.back'
                    print cmd
                    os.system(cmd)
                    # 执行远程机器文件更新操作
                    # cmd = 'scp -P 30000 ' + f_path + ' root@' + target
                    cmd = 'scp ' + f_path + ' jiangyuanheng@' + target
                    print cmd
                    os.system(cmd)
                    # reload操作
                    reload_operation(target_url, target_ip, 0)
                    # 备份文件以便之后的回滚操作；执行本地文件更新操作
                    cmd = 'cp -f ' + c_path + ' ' + cb_path + '; cp -f ' + f_path + ' ' + c_path
                    print cmd
                    subprocess.call(cmd, shell=True)
            elif op == 'rollback':  # 回滚操作
                if not me or not meb or calculate_md5_for_file(c_path) != calculate_md5_for_file(cb_path):
                    # 检查远程机器备份文件是否存在
                    # cmd = 'ssh -p 30000 root@' + target_ip + ' find ' + target_path + ' -name ' + f + '.back'
                    cmd = 'ssh jiangyuanheng@' + target_ip + ' find ' + target_path + ' -name ' + f + '.back'
                    print cmd
                    status, ret_str = commands.getstatusoutput(cmd)
                    if status != 0 or len(ret_str) == 0:  # 备份文件不存在
                        print 'Backup file does not exist.', f
                        continue

                    # cmd = 'ssh -P 30000 root@' + target_ip + ' cp ' + r_path + '.back ' + r_path
                    cmd = 'ssh jiangyuanheng@' + target_ip + ' cp ' + r_path + '.back ' + r_path
                    print cmd
                    os.system(cmd)
                    # reload操作
                    reload_operation(target_url, target_ip, 0)
                    # 执行本地文件回滚操作
                    cmd = 'cp -f ' + cb_path + ' ' + f_path + '; cp -f ' + cb_path + ' ' + c_path
                    print cmd
                    subprocess.call(cmd, shell=True)
            else:
                print 'Target file error.'


if __name__ == "__main__":
    config_file = 'conf.ini'
    conf = parse_config(config_file)
    main(conf)
    '''
    '''
