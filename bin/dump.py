#!/usr/bin/env python

from li import *
from os import path

mymysql.connect(db='testdb')
mymysql.set_table()
table = mymysql.select_rows()
for row in table:
  print row
