#!/usr/bin/env python3

import os
import yaml
import MySQLdb
import MySQLdb.cursors

if (os.path.isfile('settings.yml')):
    config = yaml.load(open('settings.yml', 'r'))

conn = MySQLdb.connect(
        host=config['db']['host'],
        user=config['db']['user'],
        passwd=config['db']['password'],
        db=config['db']['dbname'],
        charset='utf8',
        use_unicode=True,
        cursorclass=MySQLdb.cursors.DictCursor
    )

cur = conn.cursor()

print('')
cur.execute('SELECT SUM(VolBytes) as UsedBytes from Media')
used_total = cur.fetchone()['UsedBytes'] / 1024**3
print('Bacula uses %0.1fGB of storage' % used_total)

print('')
cur.execute('SELECT PoolId,CAST(Name as CHAR) as Name from Pool')
pools = cur.fetchall()
for pool in pools:
    cur.execute('SELECT SUM(VolBytes) as UsedBytes from Media where PoolId=%d' % pool['PoolId'])
    pool_used = cur.fetchone()['UsedBytes']
    if pool_used:
        print('Pool %s uses %0.1fGB' % (pool['Name'], pool_used / 1024**3))
    else:
        print('Pool %s is empty' % pool['Name'])
