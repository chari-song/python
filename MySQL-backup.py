#_*_coding:utf-8_*_
"""
@File: backup_db.py
@Email: 1044067993@qq.com
@Last Modified: 20190808
"""
import os
import datetime
from threading import Thread

HOST = '192.168.10.152'
USER = 'root'
PW = 'test@123'
PATH = '/data/mysql_backup/'
NOW = datetime.datetime.now()
NOWSTR = NOW.strftime('%Y%m%d')
CLEANSTR = (NOW - datetime.timedelta(days=5)).strftime('%Y%m%d')
DBS = [
    'zabbix'
]

def clean(db):
    database = '{dbs}'.format(dbs=db)
    format = 'rm -rf %(path)s%(db)s.%(date)s.sql'
    kv = {'path': PATH, 'date': CLEANSTR, 'db' : database}
    excute( format % kv)

def backup(db):
    database = '{dbs}'.format(dbs=db)
    format = 'mysqldump -u %(user)s -h %(host)s -p%(pw)s %(db)s > %(path)s%(db)s.%(date)s.sql'
    kv = {'user': USER, 'host': HOST, 'pw': PW, 'path': PATH, 'date': NOWSTR, 'db' : database}
    excute(format % kv)

def excute(cmd):
    os.system(cmd)

def main():
    
    dbs = range(len(DBS))
    
    def exec_task(th):
        for i in dbs:
            th[i].start()
        for i in dbs:        
            th[i].join()

    backup_t = []
    clean_t = []
    
    for i in dbs:
        t1 = Thread(target=backup, args=(DBS[i],))
        t2 = Thread(target=clean, args=(DBS[i],))
        backup_t.append(t1)
        clean_t.append(t2)
    
    exec_task(backup_t)
    exec_task(clean_t)

if __name__ == "__main__":
    main()
