#!/usr/bin/python

"""
Скрипт генерации определений объектов, отображённых в SQLFuse
"""

from __future__ import unicode_literals

import os
import sys

import xattr
import argparse
import re

from pathlib import Path
import config

class SqlTable(object):
    def __init__(self, path):
        self.path = path
        self.column_dict = dict()
        self.defaults = dict()
        self.mxl = 0
        self.cols = dict()

        for r in self.path.glob('*'):
            t = xattr.get(str(r), 'user.sqlfuse.type')
            if t == b'$L':
                x = int(xattr.get(str(r), 'user.sqlfuse.column_id'))
                self.column_dict[x] = r.name
            elif t == b'D':
                with open(str(r), 'r') as f:
                    s = f.read()
                    m = re.search('DEFAULT\s+(.+)\s+FOR\s+\[(\S+)\]\s*$', s, re.I)
                    if m is not None:
                        self.defaults[m.group(2)] = (r.name, m.group(1))
                
        for c in sorted(self.column_dict.keys()):
            pn = str(self.path.joinpath(self.column_dict[c]))
            with open(pn, 'r') as f:
                s = f.read().replace('COLUMN', '[{}]'.format(self.column_dict[c]))

                if len(s) > self.mxl:
                    self.mxl = len(s)

                b = dict({'COLUMN': s[:-1]})
                
                ds = self.defaults.get(self.column_dict[c])
                if ds is not None:
                    ct = 'CONSTRAINT [{}] DEFAULT {}'.format(ds[0], ds[1])
                    b['DEFAULT'] = ct
                    
                self.cols[c] = b

    """
    Генерирование определения таблицы
    `adjust` - отступ для колонок и ограничений `DEFAULT`
    """
    def get_definition(self, adjust = 2):
        res = 'CREATE TABLE [{}].[{}] (\n'
        res = res.format(self.path.parent.name, self.path.name)

        for c in sorted(self.cols):
            cl = self.cols[c]['COLUMN']
            res += cl.rjust(len(cl) + adjust)
            
            df = self.cols[c].get('DEFAULT')
            if df is not None:
                res += df.rjust(len(df) + (self.mxl + adjust - len(cl) - 1))

            res += ',\n'

        res = res[:-2] + '\n)'
            
        return res
                    

    def get_rights(self):
        pass

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('path')
    parser.add_argument('-f', '--full', action='store_true',
                        required=False)
    args = parser.parse_args()

    ps = args.path.split('.')
    path = Path(config.SERVERS[ps[0]][ps[1]] + '/'.join(ps[2:]))

    if xattr.getxattr(str(path), 'user.sqlfuse.type') in (b'TT', b'U'):
        so = SqlTable(path = path)
        print(so.get_definition(adjust = 2))
    
if __name__ == "__main__":
    main()
