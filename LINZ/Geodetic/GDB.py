from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import urllib2
import os.path
import json
import re
import sys
from collections import namedtuple

'''
Module to access information from the LINZ geodetic database.
'''



_cache={}
_gdburl='http://www.linz.govt.nz/gdb?mode=js&code={code}'

_defaultFileCache=".gdbjsoncache"
_useFileCache=False
_cacheFile=None
_cacheExpiry=None

def setFileCache( filename=None, expiryHours=6, useCache=True):
    global _useFileCache, _cacheFile, _cacheExpiry
    _useFileCache=useCache
    if filename is None:
        filename=os.path.join(os.path.expanduser("~"),_defaultFileCache)
    _cacheFile=filename
    _cacheExpiry=expiryHours

def getFromFileCache( code ):
    if not  _useFileCache:
        return None
    if not os.path.exists(_cacheFile):
        return None
    stndata=None
    try:
        import sqlite3
        conn=sqlite3.connect(_cacheFile)
        c=conn.cursor()
        dateoffset='-'+str(_cacheExpiry)+' hours'
        c.execute("delete from gdb_json where cachedate < date('now',?)",(dateoffset,))
        c.execute("select json from gdb_json where code=?",(code.upper(),))
        for row in c:
            stndata=row[0]
            break
        conn.commit()
    except:
        pass
    return stndata

def saveToFileCache( code, stndata ):
    if not _useFileCache:
        return
    try:
        import sqlite3
        conn=sqlite3.connect(_cacheFile)
        c=conn.cursor()
        c.execute('''
              create table if not exists gdb_json(
                  code varchar(4) not null primary key,
                  cachedate datetime not null,
                  json text not null
                 )
             ''')
        c.execute('''
                  insert or replace into gdb_json(code,cachedate,json)
                  values (?,date('now'),?)
                  ''',
                  (code.upper(),stndata))
        conn.commit()
    except:
        pass

def _json_object_hook(d): return namedtuple('X',d.keys())(*d.values())

def get( code, cache=True ):
    '''
    Retrieve information for a geodetic mark. The data is retrieved as an anonymous 
    class (constructed with named tuple) which is built from the JSON returned by the
    geodetic database 'mode=js' option.

    If cache is True then retrieved marks are saved - if the same mark is requested
    again then it is retrieved from the cache.
    '''
    if not re.match(r'^\w{4}$',code):
        raise ValueError(code+' is not a valid geodetic code')
    code=code.upper()
    if cache and code in _cache:
        stn=_cache[code]
    else:
        url=_gdburl.replace('{code}',code)
        try:
            stndata=getFromFileCache(code)
            if stndata is None:
                stndata=urllib2.urlopen(url).read()
                saveToFileCache(code,stndata)
            stn=json.loads(stndata,object_hook=_json_object_hook)
        except Exception as e:
            raise RuntimeError("Cannot connect to geodetic database: "+e.message)
        if cache:
            _cache[code]=stn
    if stn is None:
        raise ValueError(code+' is not an existing geodetic mark')
    return stn
    
