#!/usr/bin/env python

from li import * 

mymysql.connect(db='testdb')
mymysql.set_table()
to_remove = mymysql.select_rows(condition='executed=0',columns='task_id,lfn,data')
rets = {0:0,1:0,2:0}
n_removed = 0
n_requested = len(to_remove)
n_data = 0

for r in to_remove:
  lfn = r[1]
  data = r[2]
  if not data:
    retcode = invalidation.invalidate(lfn)
    rets[retcode] += 1
    if retcode==0:
      n_removed += 1
      mymysql.mark_executed(lfn)
  else:
    n_data += 1
mymysql.commit()

print 'SUMMARY'
print '\tNumber of files removed:       %i'%n_removed
print '\tNumber of files left in limbo: %i'%(n_requested-n_removed)
print '\tNumber of data files:          %i'%(n_data)
