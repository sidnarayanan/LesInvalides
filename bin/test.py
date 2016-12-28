#!/usr/bin/env python

# import mymysql as mms
# mms.connect(db='testdb')
# mms.createTable()
# mms.insertRequest('/store/mc/blah.root',False,'test mc')
# mms.insertRequest('/store/data/blah.root',True,'test data')
# mms.commit()
# mms.markExecuted('/store/mc/blah.root')
# mms.commit()
# print mms.dumpTable()

# import subprocess
# try:
#   output = subprocess.check_output('timeout 1 sleep 2',shell=True)
# except subprocess.CalledProcessError as e:
#   if not e.returncode==124: 
#     raise e
#   else:
#     print 'timed out!'

import mymysql
import invalidation


# first make some dummy requests...
mymysql.connect(db='testdb')
mymysql.create_table()
mymysql.insert_request('/store/mc/blah.root',False,'test mc')
mymysql.insert_request('/store/mc/blah2.root',False,'test mc')
mymysql.insert_request('/store/data/blah.root',True,'test data')
mymysql.mark_executed('/store/mc/blah.root')
mymysql.commit()
for r in mymysql.select_rows():
  print r
mymysql.close()

# recreate the connection and look for stuff to delete
mymysql.connect(db='testdb')
to_remove = mymysql.select_rows(condition='executed=0',columns='task_id,lfn,data')
for r in  to_remove:
  print r
  lfn = r[1]
  data = r[2]
  if not data:
    retcode = invalidation.invalidate(lfn)
    if retcode==0:
      mymysql.mark_executed(lfn)
mymysql.commit()
for r in mymysql.select_rows():
  print r