#!/usr/bin/env python


import subprocess
import os

tt_path = os.getenv('TTPATH')
db_param_path = os.getenv('DBPARAMPATH')
dbs_url = 'https://cmsweb.cern.ch/dbs/prod/global/DBSWriter'

TEST=True

def remove_tmdb(lfn):
  '''
  Remove a lfn from TMDB
  Takes the lfn as input
  Returns True if deletion succeeded, False otherwise (i.e. timeout)
  '''
  cmd = '%s/phedex/FileDeleteTMDB -db %s -list lfn:%s -invalidate'%(tt_path,db_param_path,lfn)
  try:
    if TEST:
      print cmd
      return True
    output = subprocess.check_output('timeout 90 %s'%cmd,shell=True)
    if 'finished deletion' in output:
      return True
    else:
      return False
  except subprocess.CalledProcessError as e:
    if not e.returncode==124: 
      raise e
    else:
      print 'TMDB deletion timed out! (%s)'%lfn
      return False

def remove_dbs(lfn):
  '''
  Mark as invalid in DBS
  Takes the lfn as input
  Returns True if invalidation succeeded, False otherwise
  '''
  cmd = '%s/dbs/DBS3SetFileStatus.py -url %s --status=invalid --recursive=False --files=%s'%(tt_path,dbs_url,lfn)
  try:
    if TEST:
      print cmd
      return True
    output = subprocess.check_output('timeout 90 %s'%cmd,shell=True)
    if 'All done' in output:
      return True
    else:
      return False
  except subprocess.CalledProcessError as e:
    if not e.returncode==124: 
      raise e
    else:
      print 'DBS invalidation timed out! (%s)'%lfn
      return False


def invalidate(lfn):
  '''
  Execute invalidation of an lfn
  Return values:
    0 : complete success
    1 : failed at TMDB step
    2 : TMDB succeeded but DBS failed
  '''
  tmdb_success = remove_tmdb(lfn)
  if tmdb_success:
    dbs_success = remove_dbs(lfn)
    if dbs_success:
      return 0
    else:
      return 2
  else:
    return 1