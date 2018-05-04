#!/usr/bin/env python

'''
A module that handles a single MySQL connect.
NOTE: this is implemented as a module
and not a class to avoid multiple connections
'''

import MySQLdb as ms
import time
from os import getenv

_connect = None
_cursor = None
_name = None

def connect(db,user='root',passwd=getenv('INVALIDPASSWD'),conf=None):
  global _connect, _cursor, _name
  if conf:
    conf_params = {'host':'localhost','read_default_file':conf,'db':db} 
  else:
    conf_params = {'host':'localhost','user':user,'passwd':passwd,'db':db} 
  _connect = ms.connect(**conf_params)
  _cursor = _connect.cursor()

def create_table(name='invalidations'):
  global _connect, _cursor, _name
  _name = name
  _cursor.execute('DROP TABLE IF EXISTS %s'%(name))
  return _cursor.execute('''
    CREATE TABLE IF NOT EXISTS %s (
      task_id       INT(11)      NOT NULL AUTO_INCREMENT,
      lfn           VARCHAR(400) NOT NULL,
      time_request  INT          NOT NULL,
      time_execute  INT          NOT NULL DEFAULT -1,
      description   VARCHAR(200) NOT NULL,
      executed      BOOL         NOT NULL DEFAULT 0, 
      data          BOOL         NOT NULL DEFAULT 0,
      PRIMARY KEY (task_id,lfn)
    ) ENGINE=InnoDB
    '''%(name)
    )

def set_table(name='invalidations'):
  global _name
  _name = name

def insert_request(lfn,is_data,description):
  global _connect, _cursor, _name
  if len(lfn)>400:
    print 'FATAL: lfn is too long'
    return
  if len(description)>200:
    description = description[:200]
  now = int(time.time())
  data = 1 if is_data else 0
  return _cursor.execute('''
    INSERT INTO %s (lfn, time_request, description, data)
    VALUES ('%s',%i,'%s',%i)
    '''%(_name,lfn,now,description,data)
    )

def mark_executed(lfn):
  global _connect, _cursor, _name
  return _cursor.execute('''
    UPDATE %s
    SET
      time_execute = %i,
      executed = 1
    WHERE
      lfn = '%s';
    '''%(_name,int(time.time()),lfn)
    )

def select_rows(condition=None,columns='*'):
  global _connect, _cursor, _name
  if condition:
    where = 'WHERE %s'%(condition)
  else:
    where = ''
  _cursor.execute('SELECT %s FROM %s %s;'%(columns,_name,where))
  return _cursor.fetchall()

def commit():
  global _connect, _cursor, _name
  _connect.commit()

def close():
  global _connect, _cursor, _name
  _connect.close()
  _cursor = None
  _connect = None
