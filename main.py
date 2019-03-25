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
print('Bacula is using %0.3fGB of storage' % used_total)

cur.execute('SELECT PoolId,CAST(Name as CHAR) as Name from Pool')
pools = cur.fetchall()
for pool in pools:
    print('')
    cur.execute('SELECT SUM(VolBytes) as PoolBytes from Media where PoolId=%d' % pool['PoolId'])
    pool_used = cur.fetchone()['PoolBytes']
    if pool_used:
        print('Pool %s is using %0.3fGB' % (pool['Name'], pool_used / 1024**3))
    else:
        print('Pool %s is empty' % pool['Name'])
    cur.execute('SELECT CAST(Name as CHAR) as JobName, COUNT(Name) as JobNumber, SUM(JobBytes) as JobBytes FROM Job WHERE PoolId={} GROUP BY JobName ORDER BY JobBytes DESC'.format(pool['PoolId']))
    jobs = cur.fetchall()
    for job in jobs:
        if job['JobBytes']:
            print('%40s: %5d jobs are using %0.3fGB' % (job['JobName'], job['JobNumber'], job['JobBytes'] / 1024**3))
