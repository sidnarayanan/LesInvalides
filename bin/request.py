#!/usr/bin/env python

from li import * 
from os import path

parser = argparse.ArgumentParser(description='request the invalidation of files')
parser.add_argument('--description',metavar='description',type=str)
parser.add_argument('files',metavar='files',nargs='+',type=str)
args = parser.parse_args()

if len(args.files)==1 and path.isfile(args.files[0]):
  with open(args.files[0]) as infile:
    file_list = list(infile)
else:
  file_list = args.files

mymysql.connect(db='testdb')
mymysql.set_table()
for f in file_list:
  is_data = ('/store/data' in f)
  mymysql.insert_request(f,is_data,args.description)
mymysql.commit()
mymysql.close()
