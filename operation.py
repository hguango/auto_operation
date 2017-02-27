#!/usr/bin/python
# -*- coding: utf-8 -*-
# *****************************************************************************
# 版本更新记录：
# -----------------------------------------------------------------------------
#
# 版本 1.0.0 ， 最后更新于 2016-10-11， by chuqian.liang
#
# *****************************************************************************

# =============================================================================
# 导入外部模块
# =============================================================================
#################
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import time
import datetime
import math
import os
import commands
import psycopg2

# =============================================================================
# 公共函数
# =============================================================================

# 求准确率
def predAccuracy(observ,pred,timearg):
    # observ:实际功率字典
    # pred:预测功率字典
    # timearg:计算时间点数组
    # cap:开机容量
    try:
        average = 0
        accuracy = 0
        for n in xrange(0,len(timearg)):
            a = abs(float(pred[timearg[n]][0]) - float(observ[timearg[n]]))
            average += (a / float(pred[timearg[n]][1]))**2
        accuracy = round((1 - math.sqrt(average / len(timearg))) * 1, 4)
        return accuracy
    except Exception:
        return None

#==============================================================================
#                           数据库操作类
#==============================================================================

class Oppsdba:
    def __init__(self,dba_config):
        self.dba = dba_config
        self._cursor = None
        self._conn = None
        
    def dbSetConn(self,dba):
        self._conn = psycopg2.connect(
                database = dba['database'], 
                user     = dba['user'], 
                password = dba['password'], 
                host     = dba['host'], 
                port     = dba['port']
                )
        self._cur = self._conn.cursor()
    
    def dbClose(self):
        self._cur.close()
        self._conn.close()
        
    def dbCommit(self):
        self._conn.commit()
        
    def dbSelect(self,sql):
        try:
            self.dbSetConn(self.dba)
            self._cur.execute(sql)
            select_data = self._cur.fetchall()
            self.dbClose()
        except Exception,e:
            print e
            return None
        return select_data
        
    def dbGetPhaseLine(self,sql):
        try:
            self.dbSetConn(self.dba)
            self._cur.execute(sql)
            desc = self._cur.description
            self.dbClose()
        except Exception,e:
            print e
            return None
        return desc
    def updateOpps2CdqAcc(self,fanid,tarangeid,datas):
        try:
            self.dbSetConn(self.dba)
            for key in datas.keys():
                predtime_key = key - datetime.timedelta(minutes=15) #predicttime = '00:15:00' 
                sql = """update prediction set power='%s' 
                                where predicttime='%s' and
                                moment = '%s' and 
                                tarangeid = %s and
                                fanid = %s
                                """ % (datas[key][0], 
                                       predtime_key, 
                                       key,
                                       tarangeid, 
                                       fanid
                                       )
                self._cur.execute(sql)
            self.dbCommit()
            self.dbClose()
            return True
        except Exception,e:
            print e
            return False
            
    def updateOpps2DqAcc(self,pred_time,tarangeid,datas):
        try:
            self.dbSetConn(self.dba)
            for key in datas.keys():
                sql = """update prediction set power='%s' 
                                        where predicttime='%s' and
                                        moment = '%s' and tarangeid = %s
                                """ % (datas[key][0], 
                                       pred_time,
                                       key,
                                       tarangeid, 
                                       )
                self._cur.execute(sql)
            self.dbCommit()
            self.dbClose()
            return True
        except Exception,e:
            print e
            return False
        
# *****************************************************************************
# OPPS3 
# *****************************************************************************

class Opps3Check:
   # opps3版本相关的检查函数
   def __init__(self,dbas):
       self.dbas = Oppsdba(dbas)
       
   def get3TdyObserv(self):
       # 获取今天实际功率
       try:
           today             = time.strftime('%Y-%m-%d')
           t_publish_time    = datetime.datetime.strptime(today, '%Y-%m-%d')
           tomorrow_datetime = t_publish_time + datetime.timedelta(days=1)  
           sel_sql = """select rectime,pointvalue 
                               from farmobserv 
                               where rectime > '%s' 
                               and rectime <= '%s' 
                               and pointname = 'AP' order by rectime
                               """ % (t_publish_time,tomorrow_datetime)
           today_observ_datas = self.dbas.dbSelect(sel_sql)
           return today_observ_datas
       except Exception:
           return None
   def get3YstdObserv(self):
       # 获取昨天实际功率
       try:
           today             = time.strftime('%Y-%m-%d')
           today_date        = datetime.datetime.strptime(today, '%Y-%m-%d')
           t_publish_time    = today_date - datetime.timedelta(days=1)
           sel_sql = """select rectime,pointvalue 
                               from farmobserv 
                               where rectime > '%s' 
                               and rectime <= '%s' 
                               and pointname = 'AP' order by rectime
                               """ % (t_publish_time, today_date)
           today_observ_datas = self.dbas.dbSelect(sel_sql)
           return today_observ_datas
       except Exception:
           return None
   
   # ----------------------------------- push ---------------------------------
   
   def check3predqac(self):
       # DQ准确率
       try:
           tdy_ap = self.get3TdyObserv()
           if tdy_ap is None:
               return '0||<p 连接数据库失败>'
           elif len(tdy_ap) == 0:
               return '0||<p 今日无实际功率，无法计算准确率>'
           else:
               tdy_observ = {}
               for data in tdy_ap:
                   tdy_observ[data[0]] = float(data[1])
           
           now_datetime = datetime.datetime.now()
           ystd_datetime = now_datetime - datetime.timedelta(days=1)
           pred_time = ystd_datetime.strftime('%Y-%m-%d 12:00:00')
           sel_ystd_pred = """select targettime,predpower,operatingcapacity 
                               from pred 
                               where predtype='st' 
                               and issuetime='%s' """ % pred_time
           ystd_pred = self.dbas.dbSelect(sel_ystd_pred)
           if ystd_pred is None:
               return '0||<p 连接数据库失败>'
           elif len(ystd_pred) == 0:
               return '0||<p 昨日未发布预测，无法计算准确率>'
           else:
               ystd_pred_datas = {}
               for data in ystd_pred:
                   # 时间=[预测功率，开机容量]
                   ystd_pred_datas[data[0]] = [float(data[1]), float(data[2])]
           
           tdy_time_args_st = []
           for targettime in ystd_pred_datas.keys():
               if targettime in tdy_observ.keys():
                   tdy_time_args_st.append(targettime)
           tdy_time_args_st.sort()
           acc = predAccuracy(tdy_observ,
                                  ystd_pred_datas,
                                  tdy_time_args_st)
           if acc is None:
               return '0||<p 计算DQ准确率失败>'
           else:
               acc = float(acc) * 100
               return '1||<p DQ准确率为%s%%>' % acc
       
       except Exception,e:
           return '0||<p error:%s>' % e
       
   def check3precdqac(self):
       # CDQ准确率
       try:
           tdy_ap = self.get3TdyObserv()
           if tdy_ap is None:
               return '0||<p 连接数据库失败>'
           elif len(tdy_ap) == 0:
               return '0||<p 今日无实际功率，无法计算准确率>'
           else:
               tdy_observ = {}
               for data in tdy_ap:
                   tdy_observ[data[0]] = float(data[1])
           # 获取今天提前15min的超短期预测
           today             = time.strftime('%Y-%m-%d')
           t_publish_time    = datetime.datetime.strptime(today, '%Y-%m-%d')
           tomorrow_datetime = t_publish_time + datetime.timedelta(days=1)  
           pred_today_ust_sql = """select targettime,predpower,operatingcapacity
                                   from pred 
                                   where predtype = 'ust' 
                                   and targettime > '%s'                            
                                   and targettime <= '%s'
                                   and targettime - issuetime = '00:15:00' 
                                   """ % (t_publish_time,tomorrow_datetime)
           tdy_cdq_pred = self.dbas.dbSelect(pred_today_ust_sql)
           if tdy_cdq_pred is None:
               return '0||<p 连接数据库失败>'
           elif len(tdy_cdq_pred) == 0:
               return '0||<p 未发布超短期预测，无法计算准确率>'
           else:
               tdy_cdq_datas = {}
               for data in tdy_cdq_pred:
                   # 时间=[预测功率，开机容量]
                   tdy_cdq_datas[data[0]] = [float(data[1]), float(data[2])]
           
           tdy_time_args_ust = []
           for targettime in tdy_cdq_datas.keys():
               if targettime in tdy_observ.keys():
                   tdy_time_args_ust.append(targettime)
           tdy_time_args_ust.sort()
           acc = predAccuracy(tdy_observ,
                                  tdy_cdq_datas,
                                  tdy_time_args_ust)
           if acc is None:
               return '0||<p 计算CDQ准确率失败>'
           else:
               acc = float(acc) * 100
               return '1||<p CDQ准确率为%s%%>' % acc
       
       except Exception,e:
           return '0||<p error:%s>' % e

   def check3preystddqac(self):
       # 昨日DQ准确率
       try:
           tdy_ap = self.get3YstdObserv()
           if tdy_ap is None:
               return '1||<p 连接数据库失败>'
           elif len(tdy_ap) == 0:
               return '1||<p 昨日无实际功率，无法计算准确率>'
           else:
               tdy_observ = {}
               for data in tdy_ap:
                   tdy_observ[data[0]] = float(data[1])
           
           now_datetime = datetime.datetime.now()
           ystd_datetime = now_datetime - datetime.timedelta(days=2)
           pred_time = ystd_datetime.strftime('%Y-%m-%d 12:00:00')
           sel_ystd_pred = """select targettime,predpower,operatingcapacity 
                               from pred 
                               where predtype='st' 
                               and issuetime='%s' """ % pred_time
           ystd_pred = self.dbas.dbSelect(sel_ystd_pred)
           if ystd_pred is None:
               return '1||<p 连接数据库失败>'
           elif len(ystd_pred) == 0:
               return '1||<p 前天未发布预测，无法计算准确率>'
           else:
               ystd_pred_datas = {}
               for data in ystd_pred:
                   # 时间=[预测功率，开机容量]
                   ystd_pred_datas[data[0]] = [float(data[1]), float(data[2])]
           
           tdy_time_args_st = []
           for targettime in ystd_pred_datas.keys():
               if targettime in tdy_observ.keys():
                   tdy_time_args_st.append(targettime)
           tdy_time_args_st.sort()
           acc = predAccuracy(tdy_observ,
                                  ystd_pred_datas,
                                  tdy_time_args_st)
           if acc is None:
               return '1||<p 计算昨日DQ准确率失败>'
           else:
               acc = float(acc) * 100
               return '1||<p 昨日DQ准确率为%s%%>' % acc
       
       except Exception,e:
           return '1||<p error:%s>' % e
       
   def check3preystdcdqac(self):
       # CDQ准确率
       try:
           tdy_ap = self.get3YstdObserv()
           if tdy_ap is None:
               return '1||<p 连接数据库失败>'
           elif len(tdy_ap) == 0:
               return '1||<p 昨日无实际功率，无法计算准确率>'
           else:
               tdy_observ = {}
               for data in tdy_ap:
                   tdy_observ[data[0]] = float(data[1])
           # 获取今天提前15min的超短期预测
           today             = time.strftime('%Y-%m-%d')
           t_publish_time    = datetime.datetime.strptime(today, '%Y-%m-%d')
           ystd_datetime = t_publish_time - datetime.timedelta(days=1)  
           pred_ustd_ust_sql = """select targettime,predpower,operatingcapacity
                                   from pred 
                                   where predtype = 'ust' 
                                   and targettime > '%s'                            
                                   and targettime <= '%s'
                                   and targettime - issuetime = '00:15:00' 
                                   """ % (ystd_datetime, t_publish_time)
           tdy_cdq_pred = self.dbas.dbSelect(pred_ustd_ust_sql)
           if tdy_cdq_pred is None:
               return '1||<p 连接数据库失败>'
           elif len(tdy_cdq_pred) == 0:
               return '1||<p 昨日未发布超短期预测，无法计算准确率>'
           else:
               tdy_cdq_datas = {}
               for data in tdy_cdq_pred:
                   # 时间=[预测功率，开机容量]
                   tdy_cdq_datas[data[0]] = [float(data[1]), float(data[2])]
           
           tdy_time_args_ust = []
           for targettime in tdy_cdq_datas.keys():
               if targettime in tdy_observ.keys():
                   tdy_time_args_ust.append(targettime)
           tdy_time_args_ust.sort()
           acc = predAccuracy(tdy_observ,
                                  tdy_cdq_datas,
                                  tdy_time_args_ust)
           if acc is None:
               return '1||<p 计算昨日CDQ准确率失败>'
           else:
               acc = float(acc) * 100
               return '1||<p 昨日CDQ准确率为%s%%>' % acc
       
       except Exception,e:
           return '1||<p error:%s>' % e

   # -------------------------------- predictor ------------------------------
   def check3precomm(self):
       # 检查通讯状态
       sel_sql = """select itemstatus,itemdesc from health 
                           where machinerole='predictor'
                           and itemname='health_time' """
       try:
           health_data = self.dbas.dbSelect(sel_sql)
           if health_data is None:
               return '0||<p 连接数据库失败>'
           elif len(health_data) == 0:
               return '0||<p 数据库无法查询预测服务器信息>'
           else:
               now_datetime = datetime.datetime.now()
               heath_datetime = datetime.datetime.strptime(
                                   health_data[0][1], 
                                   '%Y-%m-%d %H:%M:%S'
                                   )
               subtract = now_datetime - heath_datetime
               poor = (subtract.days * 24) + (subtract.seconds / 60)
               # 超过5分钟未更新，认为是通讯中断
               if poor >= 15:
                   return '0||<p %s通讯中断>' % health_data[0][1]
               else:
                   return '1||<p %s>' % health_data[0][1]
       except Exception,e:
           return '0||<p error:%s>' % e
           
   def check3predaemon(self):
       # 检查进程状态
       health_sql = """select itemstatus,itemdesc from health 
                           where machinerole='predictor'
                           and itemname='daemon_status' """
       try:
           health_data = self.dbas.dbSelect(health_sql)
           if health_data is None:
               return '0||<p 连接数据库失败>'
           elif len(health_data) == 0:
               return '0||<p 数据库无法查询进程状态>'
           else:
               return '%s||<p %s>' % (health_data[0][0], health_data[0][1])
       except Exception,e:
           return '0||<p error:%s>' % e
   
   def check3preservice(self):
       # 检查服务状态
       health_sql = """select itemstatus,itemdesc from health 
                           where machinerole='predictor'
                           and itemname='service_status' """
       try:
           health_data = self.dbas.dbSelect(health_sql)
           if health_data is None:
               return '0||<p 连接数据库失败>'
           elif len(health_data) == 0:
               return '0||<p 数据库无法查询服务状态>'
           else:
               return '%s||<p %s>' % (health_data[0][0], health_data[0][1])
       except Exception,e:
           return '0||<p error:%s>' % e
   
   def check3predisk(self):
       # 检查磁盘空间
       health_sql = """select itemstatus,itemdesc from health 
                           where machinerole='predictor'
                           and itemname='disk_space' """
       try:
           health_data = self.dbas.dbSelect(health_sql)
           if health_data is None:
               return '0||<p 连接数据库失败>'
           elif len(health_data) == 0:
               return '0||<p 数据库无法获取磁盘空间信息>'
           else:
               return '%s||<p %s>' % (health_data[0][0], health_data[0][1])
       except Exception,e:
           return '0||<p error:%s>' % e
   
   def check3prereport(self):
       # 检查上报
       health_sql = """select itemstatus,itemdesc from health 
                           where machinerole='predictor'
                           and itemname='report_status' """
       try:
           health_data = self.dbas.dbSelect(health_sql)
           if health_data is None:
               return '0||<p 连接数据库失败>'
           elif len(health_data) == 0:
               return '0||<p 数据库无法获取上报信息>'
           else:
               return '%s||<p %s>' % (health_data[0][0], health_data[0][1])
       except Exception,e:
           return '0||<p error:%s>' % e
           
   def check3preap(self):
       # 今日的功率
       try:
           tdy_ap = self.get3TdyObserv()
           if tdy_ap is None:
               return '0||<p get ap fail>'
           elif len(tdy_ap) == 0:
               return '0||<p 数据库无今日的实际功率>'
           else:
               # 检查上30分钟的实际功率
               cur_time = int(time.time() / 900) * 900 - 1800
               check_time = time.strftime(
                               '%Y-%m-%d %H:%M:%S', time.localtime(cur_time)
                               )
               sel_sql = """select * from farmobserv 
                                   where rectime= '%s' and pointname = 'AP'
                                   """ % check_time
               get_ap_data = self.dbas.dbSelect(sel_sql)
               if get_ap_data is None:
                   return '0||<p 获取实时功率失败>'
               elif len(get_ap_data) == 0:
                   return'0||<p 数据库[%s]实时数据中断>' % check_time
               else:
                   return '1||<p 今日功率曲线正常>'
       except Exception,e:
           return '0||<p error:%s>' % e
   
   def check3prepred(self):
       # 今日短期预测发布
       try:
           cur_moment = int(time.time())
           cur_time = time.strftime('%Y-%m-%d 13:00:00')
           pred_time = time.mktime(time.strptime(cur_time, '%Y-%m-%d %H:%M:%S'))
           if cur_moment >= pred_time:
               pulish_time = time.strftime('%Y-%m-%d 12:00:00')
               sel_sql = """select * from pred 
                               where predtype='st'
                               and issuetime='%s' 
                               """ % pulish_time
               check_tdy_pred = self.dbas.dbSelect(sel_sql)
               if check_tdy_pred is None:
                   return '0||<p 连接数据库失败>'
               elif len(check_tdy_pred) == 0:
                   return '0||<p 今日未发布短期预测>'
               else:
                   return '1||<p 今日短期预测发布成功>'
           else:
               return '1||<p 未到检测短期预测时间>'
       except Exception,e:
           return '0||<p error:%s>' % e
   
   def check3prenwp(self):
       # 今日NWP发布
       try:
           # 设置9点开始检查NWP
           cur_moment = int(time.time())
           cur_time = time.strftime('%Y-%m-%d 09:00:00')
           check_time = time.mktime(time.strptime(cur_time, '%Y-%m-%d %H:%M:%S'))
           if cur_moment < check_time:
               return '1||<p 未到检查NWP时间>'
               
           check_times = []
           check_times.append(time.strftime('%Y-%m-%d 08:00:00'))
           check_times.append(time.strftime('%Y-%m-%d 20:00:00'))
           num = len(check_times)
           for check_time in check_times:
               num -= 1
               sel_sql = """select * from nwp 
                               where issuetime='%s'
                               """ % check_time
               check_tdy_nwp = self.dbas.dbSelect(sel_sql)
               if check_tdy_nwp is None:
                   if num > 0:
                       continue
                   else:
                       return '0||<p 连接数据库失败>'
               elif len(check_tdy_nwp) == 0:
                   if num > 0:
                       continue
                   else:
                       return '0||<p 今日未发布天气预报>'
               else:
                   return '1||<p 今日已发布天气预报>'
       except Exception,e:
           return '0||<p error:%s>' % e

   def check3pretostore(self):
        # 检查tostore文件夹是否堆积文件
        try:
            cmd = 'ls /opps/process/tostore | wc -l' 
            file_count = int(commands.getoutput(cmd))
            if file_count > 150:
                return '0||<p tostore文件数已达%s>' % file_count
            return '1||<p tostore正常>'
        except Exception,e:
            return '0||<p error:%s>' % e
   # -------------------------------- collector ------------------------------
   
   def check3coldaemon(self):
       # 检查collector进程状态
       health_sql = """select itemstatus,itemdesc from health 
                           where machinerole='collector'
                           and itemname='daemon_status' """
       try:
           health_data = self.dbas.dbSelect(health_sql)
           if health_data is None:
               return '0||<c 连接数据库失败>'
           elif len(health_data) == 0:
               return '0||<c 数据库无法查询进程状态>'
           else:
               return '%s||<c %s>' % (health_data[0][0], health_data[0][1])
       except Exception,e:
           return '0||<c error:%s>' % e
   
   def check3colservice(self):
       # 检查collector服务状态
       health_sql = """select itemstatus,itemdesc from health 
                           where machinerole='collector'
                           and itemname='service_status' """
       try:
           health_data = self.dbas.dbSelect(health_sql)
           if health_data is None:
               return '0||<c 连接数据库失败>'
           elif len(health_data) == 0:
               return '0||<c 数据库无法查询进程状态>'
           else:
               return '%s||<c %s>' % (health_data[0][0], health_data[0][1])
       except Exception,e:
           return '0||<c error:%s>' % e
   
   def check3coldisk(self):
       # 检查collector磁盘状态
       health_sql = """select itemstatus,itemdesc from health 
                           where machinerole='collector'
                           and itemname='disk_space' """
       try:
           health_data = self.dbas.dbSelect(health_sql)
           if health_data is None:
               return '0||<c 连接数据库失败>'
           elif len(health_data) == 0:
               return '0||<c 数据库无法查询进程状态>'
           else:
               return '%s||<c %s>' % (health_data[0][0], health_data[0][1])
       except Exception,e:
           return '0||<c error:%s>' % e
           
   def check3colcomm(self):
       # 检查collector通讯状态
       sel_sql = """select itemstatus,itemdesc from health 
                           where machinerole='collector'
                           and itemname='health_time' """
       try:
           health_data = self.dbas.dbSelect(sel_sql)
           if health_data is None:
               return '0||<c 连接数据库失败>'
           elif len(health_data) == 0:
               return '0||<c 数据库无法查询预测服务器信息>'
           else:
               now_datetime = datetime.datetime.now()
               heath_datetime = datetime.datetime.strptime(
                                   health_data[0][1], 
                                   '%Y-%m-%d %H:%M:%S'
                                   )
               subtract = now_datetime - heath_datetime
               if subtract.days < 0:
                   return '0||<c 系统时间未对时>'
               poor = (subtract.days * 24) + (subtract.seconds / 60)
               # 超过5分钟未更新，认为是通讯中断
               if poor >= 15:
                   return '0||<c %s>' % health_data[0][1]
               else:
                   return '1||<c %s>' % health_data[0][1]
       except Exception,e:
           return '0||<c error:%s>' % e

class Opps3Handle:
    # opps3版本相关的处理函数
    def __init__(self,dbas):
       self.dbas = Oppsdba(dbas)

    def warn3preap(self):
        return [False, '无实时功率']
    def warn3prepred(self):
        return [False, '今日未发布短期预测']
    def warn3prenwp(self):
        return [False, '今日未发布NWP']
    def warn3precomm(self):
        return [False, '预测服务器通讯中断']
    def warn3pretostore(self):
        try:
            cmd = '/opps/bin/initiator restart b2'
            os.system(cmd)
            return [True, '已重启store进程'] 
        except Exception,e:
           return [False, '重启store error:%s' % e] 
        return [False, '预测服务器通讯中断']
    def warn3colcomm(self):
        return [False, '采集服务器通讯中断']
    def warn3checkip(self):
        return [False, 'connect to predictor fail!']
        

# *****************************************************************************
# OPPS2
# *****************************************************************************

class Opps2Check:
    # opps2版本相关的检查函数
    def __init__(self,dbas,cdq_acc,dq_acc):
        self.cdq_acc = cdq_acc
        self.dq_acc = dq_acc
        self.dbas = Oppsdba(dbas)
        self.setFanId()
    
    def setFanId(self):
        sel_sql = """select id from fan where idstring='@SUB' """
        fanid_data = self.dbas.dbSelect(sel_sql)
        if fanid_data is None:
            print 'can not connect to database'
            sys.exit(1)
        if len(fanid_data) == 0:
            print 'can not find fanid in database'
            sys.exit(1)
        self.fanid = fanid_data[0][0]
    
    def get2TdyObserv(self):
        try:
            today             = time.strftime('%Y-%m-%d')
            t_publish_time    = datetime.datetime.strptime(today, '%Y-%m-%d')
            tomorrow_datetime = t_publish_time + datetime.timedelta(days=1)  
            # 获取今日的实际功率
            observ_tdy_sql = """select moment,power from observ where 
                                moment > '%s' and
                                moment <= '%s' and
                                fanid = '%s'
                            """ % (t_publish_time,tomorrow_datetime,self.fanid)
            tdy_observ = self.dbas.dbSelect(observ_tdy_sql)
            return tdy_observ
        except Exception:
            return None
    
    def get2YstdObserv(self):
        try:
            today             = time.strftime('%Y-%m-%d')
            t_publish_time    = datetime.datetime.strptime(today, '%Y-%m-%d')
            ystd_datetime = t_publish_time - datetime.timedelta(days=1)  
            # 获取今日的实际功率
            observ_ystd_sql = """select moment,power from observ where 
                                moment > '%s' and
                                moment <= '%s' and
                                fanid = '%s'
                            """ % (ystd_datetime,t_publish_time,self.fanid)
            ystd_observ = self.dbas.dbSelect(observ_ystd_sql)
            return ystd_observ
        except Exception:
            return None
    
    # -------------------------------- predictor ------------------------------
    def check2predqac(self):
        # 今日DQ准确率
        try:
            tdy_ap = self.get2TdyObserv()
            if tdy_ap is None:
                return '0||<p 连接数据库失败>'
            elif len(tdy_ap) == 0:
                return '0||<p 今日无实际功率，无法计算准确率>'
            else:
                tdy_observ = {}
                for data in tdy_ap:
                    tdy_observ[data[0]] = float(data[1])
            
            now_datetime = datetime.datetime.now()
            ystd_datetime = now_datetime - datetime.timedelta(days=1)
            pred_time = ystd_datetime.strftime('%Y-%m-%d 12:00:00')
            pre_ystd_sql = ''' select moment,power,ratedpower 
                                from prediction where
                                predicttime = '%s'
                                and tarangeid = '5'
                                and fanid = '%s'
                            ''' %(pred_time, self.fanid)
            ystd_pred = self.dbas.dbSelect(pre_ystd_sql) 
            if ystd_pred is None:
                return '0||<p 连接数据库失败>'
            elif len(ystd_pred) == 0:
                return '0||<p 昨日未发布预测，无法计算准确率>'
            else:
                ystd_pred_datas = {}
                for data in ystd_pred:
                    # 时间=[预测功率，开机容量]
                    ystd_pred_datas[data[0]] = [float(data[1]), float(data[2])]
            
            tdy_time_args_st = []
            for targettime in ystd_pred_datas.keys():
                if targettime in tdy_observ.keys():
                    tdy_time_args_st.append(targettime)
            tdy_time_args_st.sort()
            acc = predAccuracy(tdy_observ,
                               ystd_pred_datas,
                               tdy_time_args_st)
            if acc is None:
                return '0||<p 计算DQ准确率失败>'
            else:
                acc = float(acc) * 100
                if acc >= self.dq_acc:
                    return '1||<p DQ准确率为%s%%>' % acc
                else:
                    return '0||<p DQ准确率为%s%%>' % acc
        except Exception,e:
            return '0||<p error:%s>' % e
            
    def check2precdqac(self):
        # 今日CDQ准确率
        try:
            tdy_ap = self.get2TdyObserv()
            if tdy_ap is None:
                return '0||<p 连接数据库失败>'
            elif len(tdy_ap) == 0:
                return '0||<p 今日无实际功率，无法计算准确率>'
            else:
                tdy_observ = {}
                for data in tdy_ap:
                    tdy_observ[data[0]] = float(data[1])
            # 获取今天提前15min的超短期预测
            today             = time.strftime('%Y-%m-%d')
            t_publish_time    = datetime.datetime.strptime(today, '%Y-%m-%d')
            tomorrow_datetime = t_publish_time + datetime.timedelta(days=1)  
            # 获取今天提前15min的超短期预测
            tdy_pre_ust_sql = """select moment,power,ratedpower 
                                from prediction where  
                                moment > '%s'                            
                                and moment <= '%s'
                                and tarangeid = '2'
                                and fanid = '%s'
                                and moment - predicttime = '00:15:00' 
                            """ % (t_publish_time,tomorrow_datetime, self.fanid)
            tdy_cdq_pred = self.dbas.dbSelect(tdy_pre_ust_sql)
            
            if tdy_cdq_pred is None:
                return '0||<p 连接数据库失败>'
            elif len(tdy_cdq_pred) == 0:
                return '0||<p 未发布超短期预测，无法计算准确率>'
            else:
                tdy_cdq_datas = {}
                for data in tdy_cdq_pred:
                    # 时间=[预测功率，开机容量]
                    tdy_cdq_datas[data[0]] = [float(data[1]), float(data[2])]
            
            tdy_time_args_ust = []
            for targettime in tdy_cdq_datas.keys():
                if targettime in tdy_observ.keys():
                    tdy_time_args_ust.append(targettime)
            tdy_time_args_ust.sort()
            acc = predAccuracy(tdy_observ,
                               tdy_cdq_datas,
                               tdy_time_args_ust)
            if acc is None:
                return '0||<p 计算CDQ准确率失败>'
            else:
                acc = float(acc) * 100
                if acc >= self.cdq_acc:
                    return '1||<p CDQ准确率为%s%%>' % acc
                else:
                    return '0||<p CDQ准确率为%s%%>' % acc
                            
        except Exception,e:
            return '0||<p error:%s>' % e

    def check2preystddqac(self):
        # 昨日DQ准确率
        try:
            tdy_ap = self.get2YstdObserv()
            if tdy_ap is None:
                return '1||<p 连接数据库失败>'
            elif len(tdy_ap) == 0:
                return '1||<p 昨日无实际功率，无法计算准确率>'
            else:
                tdy_observ = {}
                for data in tdy_ap:
                    tdy_observ[data[0]] = float(data[1])
            
            now_datetime = datetime.datetime.now()
            ystd_datetime = now_datetime - datetime.timedelta(days=2)
            pred_time = ystd_datetime.strftime('%Y-%m-%d 12:00:00')
            pre_ystd_sql = ''' select moment,power,ratedpower 
                                from prediction where
                                predicttime = '%s'
                                and tarangeid = '5'
                                and fanid = '%s'
                            ''' %(pred_time, self.fanid)
            ystd_pred = self.dbas.dbSelect(pre_ystd_sql) 
            if ystd_pred is None:
                return '1||<p 连接数据库失败>'
            elif len(ystd_pred) == 0:
                return '1||<p 前日未发布预测，无法计算准确率>'
            else:
                ystd_pred_datas = {}
                for data in ystd_pred:
                    # 时间=[预测功率，开机容量]
                    ystd_pred_datas[data[0]] = [float(data[1]), float(data[2])]
            
            tdy_time_args_st = []
            for targettime in ystd_pred_datas.keys():
                if targettime in tdy_observ.keys():
                    tdy_time_args_st.append(targettime)
            tdy_time_args_st.sort()
            acc = predAccuracy(tdy_observ,
                               ystd_pred_datas,
                               tdy_time_args_st)
            if acc is None:
                return '1||<p 计算昨日DQ准确率失败>'
            else:
                acc = float(acc) * 100
                if acc >= self.dq_acc:
                    return '1||<p 昨日DQ准确率为%s%%>' % acc
                else:
                    return '1||<p 昨日DQ准确率为%s%%>' % acc
        except Exception,e:
            return '1||<p error:%s>' % e
            
    def check2preystdcdqac(self):
        # 今日CDQ准确率
        try:
            tdy_ap = self.get2YstdObserv()
            if tdy_ap is None:
                return '1||<p 连接数据库失败>'
            elif len(tdy_ap) == 0:
                return '1||<p 昨日无实际功率，无法计算准确率>'
            else:
                tdy_observ = {}
                for data in tdy_ap:
                    tdy_observ[data[0]] = float(data[1])
            # 获取今天提前15min的超短期预测
            today             = time.strftime('%Y-%m-%d')
            t_publish_time    = datetime.datetime.strptime(today, '%Y-%m-%d')
            ystd_datetime = t_publish_time - datetime.timedelta(days=1)  
            # 获取今天提前15min的超短期预测
            tdy_pre_ust_sql = """select moment,power,ratedpower 
                                from prediction where  
                                moment > '%s'                            
                                and moment <= '%s'
                                and tarangeid = '2'
                                and fanid = '%s'
                                and moment - predicttime = '00:15:00' 
                            """ % (ystd_datetime, t_publish_time, self.fanid)
            tdy_cdq_pred = self.dbas.dbSelect(tdy_pre_ust_sql)
            
            if tdy_cdq_pred is None:
                return '1||<p 连接数据库失败>'
            elif len(tdy_cdq_pred) == 0:
                return '1||<p 昨日未发布超短期预测，无法计算准确率>'
            else:
                tdy_cdq_datas = {}
                for data in tdy_cdq_pred:
                    # 时间=[预测功率，开机容量]
                    tdy_cdq_datas[data[0]] = [float(data[1]), float(data[2])]
            
            tdy_time_args_ust = []
            for targettime in tdy_cdq_datas.keys():
                if targettime in tdy_observ.keys():
                    tdy_time_args_ust.append(targettime)
            tdy_time_args_ust.sort()
            acc = predAccuracy(tdy_observ,
                               tdy_cdq_datas,
                               tdy_time_args_ust)
            if acc is None:
                return '1||<p 计算昨日CDQ准确率失败>'
            else:
                acc = float(acc) * 100
                if acc >= self.cdq_acc:
                    return '1||<p 昨日CDQ准确率为%s%%>' % acc
                else:
                    return '1||<p 昨日CDQ准确率为%s%%>' % acc
                            
        except Exception,e:
            return '1||<p error:%s>' % e
            
    def check2precomm(self):
        # 检查通讯状态
        sel_sql = """select status,value from health 
                            where machine='predictor'
                            and item='moment' """
        try:
            health_data = self.dbas.dbSelect(sel_sql)
            if health_data is None:
                return '0||<p 连接数据库失败>'
            elif len(health_data) == 0:
                return '0||<p 数据库无法查询预测服务器信息>'
            else:
                now_datetime = datetime.datetime.now()
                heath_datetime = datetime.datetime.strptime(
                                    health_data[0][1], 
                                    '%Y-%m-%d %H:%M:%S'
                                    )
                subtract = now_datetime - heath_datetime
                poor = (subtract.days * 24) + (subtract.seconds / 60)
                # 超过5分钟未更新，认为是通讯中断
                if poor >= 15:
                    return '0||<p %s通讯中断>' % health_data[0][1]
                else:
                    return '1||<p %s>' % health_data[0][1]
        except Exception,e:
            return '0||<p error:%s>' % e
            
    def check2predaemon(self):
        # 检查进程状态
        health_sql = """select status,value from health 
                            where machine='predictor'
                            and item='daemon_status' """
        try:
            health_data = self.dbas.dbSelect(health_sql)
            if health_data is None:
                return '0||<p 连接数据库失败>'
            elif len(health_data) == 0:
                return '0||<p 数据库无法查询进程状态>'
            else:
                return '%s||<p %s>' % (health_data[0][0], health_data[0][1])
        except Exception,e:
            return '0||<p error:%s>' % e
    
    def check2preservice(self):
        # 检查服务状态
        health_sql = """select status,value from health 
                            where machine='predictor'
                            and item='service_status' """
        try:
            health_data = self.dbas.dbSelect(health_sql)
            if health_data is None:
                return '0||<p 连接数据库失败>'
            elif len(health_data) == 0:
                return '0||<p 数据库无法查询服务状态>'
            else:
                return '%s||<p %s>' % (health_data[0][0], health_data[0][1])
        except Exception,e:
            return '0||<p error:%s>' % e
    
    def check2predisk(self):
        # 检查磁盘空间
        health_sql = """select status,value from health 
                            where machine='predictor'
                            and item='memory_usage' """
        try:
            health_data = self.dbas.dbSelect(health_sql)
            if health_data is None:
                return '0||<p 连接数据库失败>'
            elif len(health_data) == 0:
                return '0||<p 数据库无法获取磁盘空间信息>'
            else:
                return '%s||<p %s>' % (health_data[0][0], health_data[0][1])
        except Exception,e:
            return '0||<p error:%s>' % e
    
    def check2prereport(self):
        # 检查上报
        lines = ''
        check_tip = 1
        sel_sql = """select * from phase"""
        try:
            # 由于OPPS2版本不同Phase表结构不同，这里需做识别
            getLine = self.dbas.dbGetPhaseLine(sel_sql)
            if getLine is None:
                return '0||<p 连接数据库失败>'
            if ('type' in getLine[0]):
                # 根据电场情况进行修改
                reporttype = ['CDQ', 'DQ']
                for rt in reporttype:
                    if rt in ['DQ',]:
                        check_time = time.strftime('%Y-%m-%d 12:00:00')
                    elif rt in ['CDQ']:
                        check_time = time.strftime('%Y-%m-%d %H:%M:%S',
                                                   time.localtime(
                                                   int(time.time()/900)*900-900
                                                   ))
                    elif rt in ['CFT','FJ']:
                        check_time = time.strftime('%Y-%m-%d %H:%M:%S',
                                                   time.localtime(
                                                   int(time.time()/300)*300-300
                                                   ))
                    check_sql = """select * from phase where type='%s' 
                                    and predicttime='%s'
                                    """ % (rt, check_time)
                    check_report = self.dbas.dbSelect(check_sql)
                    if check_report is None:
                        return '0||<p 连接数据库失败>'
                    elif len(check_report) == 0:
                        check_tip = 0
                        lines += '%s未上报|' % reporttype[rt]
                    else:
                        lines += '%s已上报|' % reporttype[rt]
                    
            elif ('tarangeid' in getLine[0]):
                # 根据电场情况进行修改
                reporttype = {2 : 'CDQ',
                              5 : 'DQ'}
                
                for rt_key in reporttype.keys():
                    if reporttype[rt_key] in ['DQ',]:
                        check_time = time.strftime('%Y-%m-%d 12:00:00')
                    elif reporttype[rt_key] in ['CDQ',]:
                        check_time = time.strftime('%Y-%m-%d %H:%M:%S',
                                                   time.localtime(
                                                   int(time.time()/900)*900-900
                                                   ))
                    check_sql = """select * from phase where tarangeid='%s' 
                                    and predicttime='%s'
                                    """ % (rt_key, check_time)
                    check_report = self.dbas.dbSelect(check_sql)
                    if check_report is None:
                        return '0||<p 连接数据库失败>'
                    elif len(check_report) == 0:
                        check_tip = 0
                        lines += '%s未上报|' % reporttype[rt_key]
                    else:
                        lines += '%s已上报|' % reporttype[rt_key]
                        
            else:
                # 海南电网eman,wenchang
                today             = time.strftime('%Y-%m-%d')
                t_publish_time    = datetime.datetime.strptime(today, '%Y-%m-%d')
                check_time = t_publish_time - datetime.timedelta(days=1) 
                check_sql = """select * from phase where predicttime='%s'
                                    """ % check_time
                check_report = self.dbas.dbSelect(check_sql)
                if check_report is None:
                    return '0||<p 连接数据库失败>'
                elif len(check_report) == 0:
                    check_tip = 0
                    lines += '[%s]未上报' % check_time
                else:
                    lines += '[%s]已上报' % check_time
                    
            return_lines = '%s||<p %s>' % (check_tip, lines)
        except Exception,e:
            return '0||<p error:%s>' % e
        return return_lines
    
    def check2preap(self):
        # 今日的功率
        try:
            tdy_ap = self.get2TdyObserv()
            if tdy_ap is None:
                return '0||<p get ap fail>'
            elif len(tdy_ap) == 0:
                return '0||<p 数据库无今日的实际功率>'
            else:
                # 检查上30分钟的实际功率
                cur_time = int(time.time() / 900) * 900 - 1800
                check_time = time.strftime(
                                '%Y-%m-%d %H:%M:%S', time.localtime(cur_time)
                                )
                sel_sql = """select * from observ 
                                    where moment= '%s' and fanid = '%s'
                                    """ % (check_time, self.fanid)
                get_ap_data = self.dbas.dbSelect(sel_sql)
                if get_ap_data is None:
                    return '0||<p 获取实时功率失败>'
                elif len(get_ap_data) == 0:
                    return '0||<p 数据库[%s]实时数据中断>' % check_time
                else:
                    return '1||<p 今日功率曲线正常>'
        except Exception,e:
            return '0||<p error:%s>' % e
            
    def check2prepred(self):
        # 今日短期预测发布
        try:
            cur_moment = int(time.time())
            cur_time = time.strftime('%Y-%m-%d 13:00:00')
            pred_time = time.mktime(time.strptime(cur_time, '%Y-%m-%d %H:%M:%S'))
            if cur_moment >= pred_time:
                pulish_time = time.strftime('%Y-%m-%d 12:00:00')
                sel_sql = """select * from prediction where
                                predicttime = '%s'
                                and tarangeid = '5'
                                and fanid = '%s'
                                """ % (pulish_time, self.fanid)
                check_tdy_pred = self.dbas.dbSelect(sel_sql)
                if check_tdy_pred is None:
                    return '0||<p 连接数据库失败>'
                elif len(check_tdy_pred) == 0:
                    return '0||<p 今日未发布短期预测>'
                else:
                    return '1||<p 今日短期预测发布成功>'
            else:
                return '1||<p 未到检测短期预测时间>'
        except Exception,e:
            return '0||<p error:%s>' % e
    
    def check2prenwp(self):
        # 今日NWP发布
        try:
            # 9点后开始检查NWP发布情况
            check_time = time.strftime('%Y-%m-%d 09:00:00')
            check_stamp = time.mktime(time.strptime(check_time, '%Y-%m-%d %H:%M:%S'))
            now_stamp = int(time.time())
            if now_stamp < check_stamp:
                return '1||<p 未到检查NWP时间>'
            health_sql = """select status,value from health 
                            where machine='predictor'
                            and item='nwp_arrival' """
            check_nwp = self.dbas.dbSelect(health_sql)
            if check_nwp is None:
                return '0||<p 连接数据库失败>'
            elif len(check_nwp) == 0:
                # 2.4版本的健康表中无nwp状态，只能到数据库里查找
                nwp_time = time.strftime('%Y-%m-%d 00:15:00')
                nwp_sql = """ select * from nwp where predicttime = '%s'
                            """ % nwp_time
                nwp_data = self.dbas.dbSelect(nwp_sql)
                if nwp_data is None:
                    return '0||<p 连接数据库失败>'
                elif len(nwp_data) == 0:
                    return '0||<p 数据库无NWP数据>'
                else:
                    return '1||<p NWP已发布[%s]数据>' % nwp_time
            else:
                return '%s||<p NWP%s>' % (check_nwp[0][0], check_nwp[0][1])
        except Exception,e:
            return '0||<p error:%s>' % e
            
    # -------------------------------- collector ------------------------------
    
    def check2coldaemon(self):
        # 检查collector进程状态
        health_sql = """select status,value from health 
                            where machine='collector'
                            and item='daemon_status' """
        try:
            health_data = self.dbas.dbSelect(health_sql)
            if health_data is None:
                return '0||<c 连接数据库失败>'
            elif len(health_data) == 0:
                return '0||<c 数据库无法查询进程状态>'
            else:
                return '%s||<c %s>' % (health_data[0][0], health_data[0][1])
        except Exception,e:
            return '0||<c error:%s>' % e
    
    def check2colservice(self):
        # 检查collector服务状态
        health_sql = """select status,value from health 
                            where machine='collector'
                            and item='service_status' """
        try:
            health_data = self.dbas.dbSelect(health_sql)
            if health_data is None:
                return '0||<c 连接数据库失败>'
            elif len(health_data) == 0:
                # 2.4版本的collector不检查service状态,这里不做检查
                return '1||<c 数据库无法查询进程状态>'
            else:
                return '%s||<c %s>' % (health_data[0][0], health_data[0][1])
        except Exception,e:
            return '0||<c error:%s>' % e
    
    def check2coldisk(self):
        # 检查collector磁盘状态
        health_sql = """select status,value from health 
                            where machine='collector'
                            and item='memory_usage' """
        try:
            health_data = self.dbas.dbSelect(health_sql)
            if health_data is None:
                return '0||<c 连接数据库失败>'
            elif len(health_data) == 0:
                return '0||<c 数据库无法查询进程状态>'
            else:
                return '%s||<c %s>' % (health_data[0][0], health_data[0][1])
        except Exception,e:
            return '0||<c error:%s>' % e
            
    def check2colcomm(self):
        # 检查collector通讯状态
        health_sql = """select status,value from health 
                            where machine='collector'
                            and item='moment' """
        try:
            health_data = self.dbas.dbSelect(health_sql)
            if health_data is None:
                return '0||<c 连接数据库失败>'
            elif len(health_data) == 0:
                return '0||<c 数据库无法查询预测服务器信息>'
            else:
                now_datetime = datetime.datetime.now()
                heath_datetime = datetime.datetime.strptime(
                                    health_data[0][1], 
                                    '%Y-%m-%d %H:%M:%S'
                                    )
                subtract = now_datetime - heath_datetime
                poor = (subtract.days * 24) + (subtract.seconds / 60)
                # 超过5分钟未更新，认为是通讯中断
                if poor >= 15:
                    return '0||<c %s>' % health_data[0][1]
                else:
                    return '1||<c %s>' % health_data[0][1]
        except Exception,e:
            return '0||<c error:%s>' % e

class Opps2Handle:
    # opps2版本相关的处理函数
    def __init__(self,dbas,farm_code,cdq_acc,dq_acc):
        self.a = 0       # 预测的系数
        self.b = 0       # 实际功率的系数
        self.d = 0    # 每次系数调整的幅度
        self.farm_code = farm_code
        self.total_cap = 0
        self.cdq_acc_rep = cdq_acc
        self.dq_acc_rep = dq_acc
        self.dbas = Oppsdba(dbas)
        self.setFanId()
        #self.setCap()

#    def setCap(self):
#        try:
#            # 获取电场的总装机容量
#            sql = '''select ratedpower from farm where idstring = '%s'
#            ''' % self.farm_code
#            get_cap = self.dbas.dbSelect(sql)
#            if get_cap is None:
#                print 'setCap:连接数据库失败'
#            elif len(get_cap) == 0:
#                print '数据库无法查询装机容量，无法计算准确率'
#            else:
#                self.total_cap = get_cap[0][0]
#        except Exception,e:
#            print 'setCap Error:%s' % str(e)
    def setconfig(self):
        self.a = 1       # 预测的系数
        self.b = 0       # 实际功率的系数
        self.d = 0.02    # 每次系数调整的幅度
 
    def setFanId(self):
        sel_sql = """select id from fan where idstring='@SUB' """
        fanid_data = self.dbas.dbSelect(sel_sql)
        if fanid_data is None:
            print 'can not connect to database'
            sys.exit(1)
        if len(fanid_data) == 0:
            print 'can not find fanid in database'
            sys.exit(1)
        self.fanid = fanid_data[0][0]
        
    def get2TdyObserv(self):
        try:
            today             = time.strftime('%Y-%m-%d')
            t_publish_time    = datetime.datetime.strptime(today, '%Y-%m-%d')
            tomorrow_datetime = t_publish_time + datetime.timedelta(days=1)
            # 获取今日的实际功率
            observ_tdy_sql = """select moment,power from observ where 
                                moment > '%s' and
                                moment <= '%s' and
                                fanid = '%s'
                            """ % (t_publish_time,tomorrow_datetime,self.fanid)
            tdy_observ = self.dbas.dbSelect(observ_tdy_sql)
            return tdy_observ
        except Exception:
            return None
    
    def warn2predqac(self):
        # OPPS2短期准确率修改函数
        tarangeid = 5
        if self.farm_code not in ['wenchang','eman']:
            cur_moment = int(time.time())
            cur_time = time.strftime('%Y-%m-%d 08:00:00')
            check_time = time.mktime(time.strptime(cur_time, '%Y-%m-%d %H:%M:%S'))
            if cur_moment < check_time:
                return [True, '未到时间处理短期准确率故障']
                
            return [False, '短期准确率故障问题不做任何处理']
        # 未避免还未生成第一个实际功率的点就检查出故障，这里设置为凌晨1点才开始处理
#        if (int(time.strftime('%H')) > 1):
#            return [True, '未到检查时间不做处理']
        try:
            self.setconfig()
            tdy_ap = self.get2TdyObserv()
            if tdy_ap is None:
                return [False, '连接数据库失败']
            elif len(tdy_ap) == 0:
                return [False, '今日无实际功率，无法计算准确率']
            else:
                tdy_observ = {}
                for data in tdy_ap:
                    tdy_observ[data[0]] = float(data[1])
            
            now_datetime = datetime.datetime.now()
            ystd_datetime = now_datetime - datetime.timedelta(days=1)
            pred_time = ystd_datetime.strftime('%Y-%m-%d 12:00:00')
            pre_ystd_sql = ''' select moment,power,ratedpower 
                                from prediction where
                                predicttime = '%s'
                                and tarangeid = '%s'
                                and fanid = '%s'
                            ''' %(pred_time, 
                                  tarangeid,                                  
                                  self.fanid)
            ystd_pred = self.dbas.dbSelect(pre_ystd_sql) 
            if ystd_pred is None:
                return [False, '连接数据库失败']
            elif len(ystd_pred) == 0:
                return [False, '昨日未发布预测，无法计算准确率']
            else:
                ystd_pred_datas = {}
                for data in ystd_pred:
                    # 时间=[预测功率，开机容量]
                    ystd_pred_datas[data[0]] = [float(data[1]), float(data[2])]
            
            tdy_time_args_st = []
            for targettime in ystd_pred_datas.keys():
                if targettime in tdy_observ.keys():
                    tdy_time_args_st.append(targettime)
            tdy_time_args_st.sort()
            while(True):
                acc = predAccuracy(tdy_observ, ystd_pred_datas, tdy_time_args_st)
                if acc is None:
                    return [False, '计算DQ准确率失败']
                else:
                    acc = float(acc) * 100
                if acc > self.dq_acc_rep:
                    print '已生成准确率为:%s%%的预测数据' % acc
                    break
                else:
                    # 通过调整系数进行修改数据
                    print '当前准确率为:%s%%,目标准确率为:%s%%,正在修改系数...' % (acc, self.dq_acc_rep)
                    self.a -= self.d
                    self.b += self.d
                    
                    args = {}
                    for n in xrange(0, len(tdy_time_args_st)):
                        if n == 0:
                            t = tdy_time_args_st[0]
                            args[t] = [self.a * ystd_pred_datas[t][0] + self.b * tdy_observ[t], 
                                       ystd_pred_datas[t][1]
                                       ]
                        elif n == (len(tdy_time_args_st) - 1):
                            t = tdy_time_args_st[n]
                            args[t] = [self.a * ystd_pred_datas[t][0] + self.b * tdy_observ[t], 
                                       ystd_pred_datas[t][1]
                                       ]
                        else:
                            t = tdy_time_args_st[n]
                            t1 = tdy_time_args_st[n - 1]
                            t2 = tdy_time_args_st[n + 1]
                            pred = (ystd_pred_datas[t1][0] + ystd_pred_datas[t][0] + ystd_pred_datas[t2][0]) / 3
                            observ = (tdy_observ[t1] + tdy_observ[t] + tdy_observ[t2]) /3
                            args[t] = [self.a * pred + self.b * observ, 
                                       ystd_pred_datas[t][1]
                                       ]
                    
                    ystd_pred_datas = args
            update = self.dbas.updateOpps2DqAcc(pred_time, tarangeid, ystd_pred_datas)
            if update:
                print '已成功修改预测值'
                return [True, '已生成准确率为:%s%%的预测数据' % acc]
            else:
                return [False, '修改预测数据失败']
        except Exception,e:
            print 'warn2predqac error:%s' % str(e)
            return [False ,'error:%s' % e]
        
        
    def warn2precdqac(self):
        # OPPS2超短期准确率修改函数超短期准确率故障问题不做任何处理
        try:
            tarangeid = 2
            if self.farm_code not in ['wenchang','eman']:
                cur_moment = int(time.time())
                cur_time = time.strftime('%Y-%m-%d 08:00:00')
                check_time = time.mktime(time.strptime(cur_time, '%Y-%m-%d %H:%M:%S'))
                if cur_moment < check_time:
                    return [True, '未到时间处理超短期准确率故障']
                return [False, '超短期准确率故障问题不做任何处理']
            # 未避免还未生成第一个实际功率的点就检查出故障，这里设置为凌晨1点才开始处理
#            if (int(time.strftime('%H')) > 1):
#                return [True, '未到检查时间不做处理']
            self.setconfig()
            tdy_ap = self.get2TdyObserv()
            if tdy_ap is None:
                return [False, '连接数据库失败']
            elif len(tdy_ap) == 0:
                return [False, '今日无实际功率，无法计算准确率']
            else:
                tdy_observ = {}
                for data in tdy_ap:
                    tdy_observ[data[0]] = float(data[1])
            # 获取今天提前15min的超短期预测
            today             = time.strftime('%Y-%m-%d')
            t_publish_time    = datetime.datetime.strptime(today, '%Y-%m-%d')
            tomorrow_datetime = t_publish_time + datetime.timedelta(days=1)  
            # 获取今天提前15min的超短期预测
            tdy_pre_ust_sql = """select moment,power,ratedpower 
                                from prediction where  
                                moment > '%s'                            
                                and moment <= '%s'
                                and tarangeid = '%s'
                                and fanid = '%s'
                                and moment - predicttime = '00:15:00' 
                            """ % (t_publish_time,
                                   tomorrow_datetime, 
                                   tarangeid, 
                                   self.fanid
                                   )
            tdy_cdq_pred = self.dbas.dbSelect(tdy_pre_ust_sql)
            
            if tdy_cdq_pred is None:
                return [False, '连接数据库失败']
            elif len(tdy_cdq_pred) == 0:
                return [False, '昨日未发布预测，无法计算准确率']
            else:
                tdy_cdq_datas = {}
                for data in tdy_cdq_pred:
                    # 时间=[预测功率，开机容量]
                    tdy_cdq_datas[data[0]] = [float(data[1]), float(data[2])]
            
            tdy_time_args_ust = []
            for targettime in tdy_cdq_datas.keys():
                if targettime in tdy_observ.keys():
                    tdy_time_args_ust.append(targettime)
            tdy_time_args_ust.sort()
            while(True):
                acc = predAccuracy(tdy_observ,
                                   tdy_cdq_datas,
                                   tdy_time_args_ust)
                if acc is None:
                    return [False, '计算CDQ准确率失败']
                else:
                    acc = float(acc) * 100
                if acc > self.cdq_acc_rep:
                    print '已生成准确率为:%s%%的预测数据' % acc
                    break
                else:
                    # 通过调整系数进行修改数据
                    print '当前准确率为:%s%%,目标准确率为:%s%%,正在修改系数...' % (acc, self.dq_acc_rep)
                    self.a -= self.d
                    self.b += self.d
                    
                    args = {}
                    for n in xrange(0, len(tdy_time_args_ust)):
                        if n == 0:
                            t = tdy_time_args_ust[0]
                            args[t] = [self.a * tdy_cdq_datas[t][0] + self.b * tdy_observ[t], 
                                       tdy_cdq_datas[t][1]
                                       ]
                        elif n == (len(tdy_time_args_ust) - 1):
                            t = tdy_time_args_ust[n]
                            args[t] = [self.a * tdy_cdq_datas[t][0] + self.b * tdy_observ[t], 
                                       tdy_cdq_datas[t][1]
                                       ]
                        else:
                            t = tdy_time_args_ust[n]
                            t1 = tdy_time_args_ust[n - 1]
                            t2 = tdy_time_args_ust[n + 1]
                            pred = (tdy_cdq_datas[t1][0] + tdy_cdq_datas[t][0] + tdy_cdq_datas[t2][0]) / 3
                            observ = (tdy_observ[t1] + tdy_observ[t] + tdy_observ[t2]) /3
                            args[t] = [self.a * pred + self.b * observ, 
                                       tdy_cdq_datas[t][1]
                                       ]
                    
                    tdy_cdq_datas = args    
            update = self.dbas.updateOpps2CdqAcc(self.fanid, tarangeid, tdy_cdq_datas)
            if update:
                print '已成功修改预测值'
                return [True, '已生成准确率为:%s%%的预测数据' % acc]
            else:
                return [False, '修改预测数据失败']
        except Exception,e:
            print 'warn2precdqac error:%s' % str(e)
            return [False ,'error:%s' % e]
                
    def warn2preap(self):
        return [False, '无实时功率']
    def warn2prepred(self):
        return [False, '今日未发布短期预测']
    def warn2prenwp(self):
        return [False, '今日未发布NWP']
    def warn2precomm(self):
        return [False, '预测服务器通讯中断']
    def warn2colcomm(self):
        return [False, '采集服务器通讯中断']
    def warn2checkip(self):
        return [False, 'connect to predictor fail!']

        



















                
