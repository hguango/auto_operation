#!/usr/bin/python
# -*- coding: utf-8 -*-
# *****************************************************************************
# 版本更新记录：
# -----------------------------------------------------------------------------
#
# 版本 1.0.0 ， 最后更新于 2016-04-11， by chuqian.liang
#
# *****************************************************************************
# =============================================================================
# 设置
# =============================================================================

CURRENT_VERSION = '1.0.0'
interval = 300   #循环控制时间
              
# =============================================================================
# 导入外部模块
# =============================================================================

import sys
reload(sys)
sys.setdefaultencoding('utf8')
import os
import time
import re
import commands
import ConfigParser
import operation
# =============================================================================
# 公共函数
# =============================================================================

def Usage(version=CURRENT_VERSION):
    print """fault_handle %s
Usage:
    python fault_handle.py -dmid daemon_id
Warn:
    本程序支持OPPS2|OPPS3
""" % (version)
    sys.exit(1)

def checkDmid(dmid):
    # dmid检查 
    cmd = 'ps -e -o cmd --no-headers | grep -E "%s"' % dmid
    res = commands.getoutput(cmd)
    count = 0
    for line in res.split('\n'):
        line = line.strip()
        if re.match(r'^.*-dmid[ ]%s' % dmid, line) is not None:
            count += 1
    if count > 1:
        return True
    else:
        return False

def getCfg():
    cfg_dist = {}
    cfg_path = "../config/check.cfg"
    if not os.path.exists(cfg_path):
        print 'do not find file[%s]' % cfg_path
        sys.exit(1)
    # 检查项
    check_dist = {'basic_set' : ['opps_user',
                                'opps_edition',
                                'farm_code'], 
                  'database_info' : ['db_host', 
                                    'db_port', 
                                    'db_user', 
                                    'db_passwd', 
                                    'db_database'],
                   'cloud_set' : ['cloud_ip', 
                                'cloud_port', 
                                'cloud_user', 
                                'cloud_passwd', 
                                'cloud_remote_path', 
                                'cloud_local_path'],
                    }
    cf = ConfigParser.ConfigParser()
    cf.read(cfg_path)
    secs = cf.sections()  # ['basic_set', 'database_info', 'cloud_set']
    for item in check_dist.keys():
        if item not in secs:
            print '[%s] has no [%s]' % (cfg_path, item)
            sys.exit(1)
        cfg_dist[item] = {}
        cfg_items = cf.options(item)
        # 获取check_dist[item]中有,而cfg_items中没有项
        item_diff = list(set(check_dist[item]).difference(set(cfg_items)))
        if len(item_diff) != 0:
            print 'cfg_path miss item %s' % item_diff
            sys.exit(1)
        
        # 检查配置项
        for datail_item in check_dist[item]:
            item_char = (cf.get(item, datail_item)).split('#')[0].lstrip(' ').rstrip(' ')
            cfg_dist[item][datail_item] = item_char
            if len(cfg_dist[item][datail_item]) == 0:
                print 'cfg_path[%s] miss item [%s]' % (item, datail_item)
                sys.exit(1)
    return cfg_dist
    
def outPutFile(file_path, filename, file_datas):
    # 生成文件函数 ，数据内容编码：GBK
    try:
        in_filename = '.in.' + filename
        if not os.path.isdir(file_path):
            os.makedirs(file_path)
        temp_file = os.path.join(file_path, in_filename)
        result_file = os.path.join(file_path, filename)
        file_datas = file_datas.decode('utf-8').encode('gbk')
        with open(temp_file, 'wb') as f:
            f.writelines(file_datas)
        os.rename(temp_file, result_file)
        print '已成功生成文件[%s]' % result_file
        return 'OK'
    except Exception,e:
        print '生成[%s]文件失败' % result_file
        print 'error:%s' % str(e)
        return 'NO'

class Operation:
    def __init__(self,dist):
        self.tmp_dir = ''
        self.farm_code = ''
        self.content = ''
        self.filename = ''
        self.edition = 0
        self.cdq_acc = 85   # 超短期的准确率修改标准
        self.dq_acc = 75    # 短期的准确率修改标准
        self.cdq_acc_rep = 95   # 超短期修改后的准确率，一般只用在海南电网
        self.dq_acc_rep = 87    # 短期修改后的准确率，一般只用在海南电网
        self.dbas = None
        self.checkitem = None
        self.opps2_check = None
        self.opps3_check = None
        self.opps2_handle = None
        self.opps3_handle = None
        self.setInitialize(dist)
        

    def setInitialize(self,dist):
        # 初始化设置
        self.tmp_dir = dist['cloud_set']['cloud_local_path']
        self.farm_code = dist['basic_set']['farm_code']
        self.edition = dist['basic_set']['opps_edition']
        self.setHipgACC()
        self.dbas = {
                'host' : dist['database_info']['db_host'],
                'port' : dist['database_info']['db_port'],
                'user' : dist['database_info']['db_user'],
            'password' : dist['database_info']['db_passwd'],
            'database' : dist['database_info']['db_database']
        }
        # push为必须推送信息区域#
        # 例子：opps2.check2.PreDQAC()
        # 请保证数组里加的字符所组成的check函数已添加
        # 由于选用ConfigParser模块，函数名中的字母只能用小写
        self.checkitem = {
            'push' : ['predqac', 
                    'precdqac',
                    'preystddqac',
                    'preystdcdqac',
                    ],
            'health' : ['preap', 
                      'prepred', 
                      'prenwp',
                      'precomm',
                      'prereport',
                      'predaemon',
                      'preservice',
                      'predisk',
                      'pretostore',
                      'colcomm',
                      'coldaemon',
                      'colservice',
                      'coldisk',
                      ]
            }
        if self.edition == '3':
            self.opps3_check = operation.Opps3Check(self.dbas)
            self.opps3_handle = operation.Opps3Handle(self.dbas)
        elif self.edition == '2':
            self.opps2_check = operation.Opps2Check(self.dbas,
                                                    self.cdq_acc,
                                                    self.dq_acc
                                                    )
            self.opps2_handle = operation.Opps2Handle(self.dbas, 
                                                      self.farm_code,
                                                      self.cdq_acc_rep,
                                                      self.dq_acc_rep
                                                      )
        else:
            Usage()
    
    def setHipgACC(self):
        # 针对海南电网重新设置准确率的标准
        hipg_farm = ['eman', 'wenchang']
        if self.farm_code in hipg_farm:
            self.cdq_acc = 90   # 超短期的准确率修改标准
            self.dq_acc = 83    # 短期的准确率修改标准
        
    def makeCheckFile(self):
        # 调用operation进行检查并生成健康文件
        try:
            self.content = ''
            ln = '\r\n'
            ping_ok = self.checkPing()
            if ping_ok:
                # 请保证数组里加的字符所组成的函数已添加
                for key in self.checkitem.keys():
                    self.content += '[%s]%s' % (key, ln)
                    for item in self.checkitem[key]:
                        func_string = "self.opps%s_check.check%s%s()" % (
                                    self.edition, 
                                    self.edition, 
                                    item 
                                    )
                        self.content += "%s=%s%s" % (item, eval(func_string), ln)
            else:
                self.content += "[push]%s" % ln
                self.content += "checkip=0||<c 预测服务器连接中断>%s" % ln
                
            self.setFileName()
            outPutFile(self.tmp_dir, 
                       self.filename, 
                       self.content)
        except Exception,e:
            print '[makeCheckFile].Error:%s' % str(e)
    
    def setFileName(self):
        self.filename = 'checked_%s_%s' % (self.farm_code, 
                            time.strftime('%Y%m%d_000000')
                            )
                            
    def findCheckFile(self):
        # ======|从健康文件中获取告警信息|======
        check_path = os.path.join(self.tmp_dir, self.filename)
        #print 'check_path[%s]' % check_path
        if not os.path.exists(check_path):
            print 'do not find[%s],continue' % check_path
            return False
        else:
            return True
    
    def getWarn(self):
        try:
            self.setFileName()
            filepath = os.path.join(self.tmp_dir, self.filename)
            gw = ConfigParser.ConfigParser()
            gw.read(filepath)
            secs = gw.sections()
            check_datas = {}
            for sec in secs:
                check_items = gw.options(sec)
                for item in check_items:
                    item_char = gw.get(sec, item)
                    split_value = item_char.split('||')
                    check_datas[item] = split_value
            warn_item = []
            for check_key in check_datas.keys():
                if check_datas[check_key][0] == '0':
                    warn_item.append(check_key)
                    
            return warn_item
        except Exception,e:
            print e
            return None
    
    def fuckWarn(self,warn_list):
        # 告警处理函数
        # 只处理must_handle数组里的告警故障
        # 确认数组里组成的warn处理函数已经添加
        needwarn = ''
        needmark = []
        num = 1
        must_handle = ['predqac',
                       'precdqac',
                       'preap',
                       'prepred',
                       'prenwp',
                       'precomm',
                       'colcomm',
                       'checkip',
                       ]

        for warn in warn_list:
            # 检查该故障今天是否处理过
            # 2016-10-13.predqac.warnmark
            had_mark = self.hadMark(warn)
            if had_mark:
                continue
            fuck_warn_success = True
            try:
                warn_fanc = 'self.opps%s_handle.warn%s%s()' % (
                                    self.edition, 
                                    self.edition, 
                                    warn
                                    )
                if warn in must_handle:
                    [fuck_warn_success, handle_result] = eval(warn_fanc)
            except Exception,e:
                # 报错则不做任何处理，跳过
                print '[eval(%s)]error:%s' % (warn_fanc, str(e))
                continue
            # 如果处理失败，则生成告警文件
            if not fuck_warn_success:
                msg = '%s=%s\t%s\r\n' % (warn, num, handle_result)
                num += 1
                needwarn += msg
                needmark.append(warn)
        
        if len(needwarn) != 0:
            self.makeWarnFile(needwarn)
        for warn in needmark:
            self.makeMark(warn)
            time.sleep(1)
    
    def makeWarnFile(self, msg):
        # 生成告警文件
        name = 'warn_%s_%s' % (self.farm_code, time.strftime('%Y%m%d_%H%M%S'))
        ln = '\r\n'    
        content = '[warn]%s' % ln
        content +=  msg
        outPutFile(self.tmp_dir, name, content)
        
    
    def hadMark(self, warn):
        # 检查标记文件
        # 2016-10-13.predqac.warnmark
        mark_name = '%s.%s.warnmark' % (time.strftime('%Y-%m-%d'), warn)
        mark_path = os.path.join(self.tmp_dir, mark_name)
        if os.path.exists(mark_path):
            print 'warn[%s] had handled' % warn
            return True
        return False

    def makeMark(self, warn):
        # 生成标记文件
        try:
            # 2016-10-13.predqac.warnmark
            mark_name = '%s.%s.warnmark' % (time.strftime('%Y-%m-%d'), warn)
            mark_path = os.path.join(self.tmp_dir, mark_name)
            mark_f = file(mark_path, 'wb')
            mark_f.close()
            return True
        except Exception,e:
            print 'markMark error:%s' % str(e)
            return False
            
    def checkPing(self):
        # 检查预测服务器的通讯
        # 检查结果只有等于0的时候为正常
        ping_str = 'ping -c2 %s' % self.dbas['host']
        ping_result = os.system(ping_str)
        if ping_result:
            return False
        else:
            return True
        
    def del_ystd_file(self):
        # 删除过时的文件
        file_list = os.listdir(self.tmp_dir)
        for filename in file_list:
            match_string = 'checked_%s_(\d{8}_\d{6})$' % self.farm_code
            mark_string = '(\d{4}-\d{2}-\d{2})\.(.+)\.warnmark$'
            find_file = re.match(match_string, filename)
            find_mark = re.match(mark_string, filename)
            if find_file:
                filetime = find_file.group(1)
                file_stamp = time.mktime(time.strptime(filetime,'%Y%m%d_%H%M%S'))
                cur_m = int(time.time())
                if (cur_m - file_stamp) > 86400:
                    os.remove(os.path.join(self.tmp_dir,filename))
            if find_mark:
                filetime = find_mark.group(1)
                file_stamp = time.mktime(time.strptime(filetime,'%Y-%m-%d'))
                cur_m = int(time.time())
                if (cur_m - file_stamp) > 86400:
                    os.remove(os.path.join(self.tmp_dir,filename))
                
        
# =============================================================================
# 防止被import
# =============================================================================

if __name__ != '__main__':
    print 'has import '
    sys.exit(1)

# =============================================================================
# 检查东八时区
# =============================================================================

if time.timezone != -28800:
    print >> sys.stderr, "当前系统时区不是东八区, 将影响程序正常运行, 请修改操作系统中的时区设置!"
    sys.exit(1)

# =============================================================================
# 路径检查
# =============================================================================

# 当前执行路径
now_path = os.getcwd()
# 程序路径
program_path = os.path.split(os.path.realpath(sys.argv[0]))[0]
if now_path != program_path:
    print 'please Execute [operation_check.py] in [%s]' % program_path
    sys.exit(1)

# =============================================================================
# 处理命令行参数
# =============================================================================

argv = sys.argv
MY_DMID = None
MY_DMNO = None
if len(argv) < 2:
    Usage()
i = 1
try:
    while i < len(argv):
        arg = argv[i]
        if arg == '-dmid':
            i = i + 1
            MY_DMID = argv[i]
        elif arg == '-dmno':
            i = i + 1
            MY_DMNO = argv[i]
        else:
            Usage()
        i = i + 1
except SystemExit:
    sys.exit(1)
except:
    Usage()

if MY_DMID is None:
    Usage()
    
# =============================================================================
# 防止多实例
# =============================================================================

if checkDmid(MY_DMID):
    print >> sys.stderr, "当前进程已有其它实例在运行，本进程退出。"
    sys.exit(1)

# =============================================================================
# 读取配置文件
# =============================================================================

big_dist = getCfg()

# =============================================================================
# 用户权限检查
# =============================================================================

execute_user = big_dist['basic_set']['opps_user']
if os.popen('id -un').read().strip('\n') != execute_user:
    print >> sys.stderr, "请以%s身份执行本脚本." % execute_user
    sys.exit(1)

# =============================================================================
# 设置
# =============================================================================

op = Operation(big_dist) 
 
# =============================================================================
# 工业循环
# =============================================================================

while(True):
    try:
        # 本程序设定只会在00:30后开始工作
        cur_moment = int(time.time())
        nowtime_str = time.strftime('%Y-%m-%d 00:30:00')
        time_array = time.strptime(nowtime_str, "%Y-%m-%d %H:%M:%S")
        time_stamp = int(time.mktime(time_array))
        if cur_moment < time_stamp:
            print 'Not yet time to check...'
            time.sleep(interval)
            continue
        
        # 调用operation进行检查并生成健康文件
        op.makeCheckFile()
        
        # 检查健康文件是否存在
        if not op.findCheckFile():
            time.sleep(interval)
            continue
        
        warn_list = op.getWarn()
        if warn_list is None:
            # 生成标记文件
            warn = 'system_error'
            had_mark = op.hadMark(warn)
            if had_mark:
                time.sleep(interval)
                continue
            else:
                # 生成故障告警文件
                msg = 'system_error=无法处理健康文件\r\n'
                op.makeWarnFile(msg)
                op.makeMark(warn)
        elif len(warn_list) != 0:
            op.fuckWarn(warn_list)
        else:
            # 无故障
            pass
        op.del_ystd_file()   # 删除不是今天生成的健康文件、告警和标记文件
        time.sleep(interval)
    except (KeyboardInterrupt, SystemExit):
       break
    except:
       time.sleep(1)
        
