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

def setCached( filename=None, expiryHours=6, useCache=True, purge=False):
    '''
    Initiate the use of an persistent file cache.  Subsequent requests
    for station information may be filled from the cache, and will
    be saved to the cache.  

    The default cache is ~/.gdbjsoncache.  It is structured as an 
    sqlite database.

    expiryHours is the maximum age that cached data will be used (though 
    older data may be used if it cannot be retrieved)

    useCache can be used to cancel using the cache (set to False)

    purge if set will purge old data from the cache
    '''
    global _useFileCache, _cacheFile, _cacheExpiry
    _useFileCache=useCache
    if filename is None:
        filename=os.path.join(os.path.expanduser("~"),_defaultFileCache)
    _cacheFile=filename
    _cacheExpiry=expiryHours
    if purge:
        try:
            import sqlite3
            conn=sqlite3.connect(_cacheFile)
            c=conn.cursor()
            dateoffset='-'+str(_cacheExpiry)+' hours'
            c.execute("delete from gdb_json where cachedate < datetime('now',?)",(dateoffset,))
            conn.commit()
        except:
            pass

def _getFromFileCache( code, haveConnection=False, useExpired=False ):
    if not  _useFileCache:
        return None, False
    if not os.path.exists(_cacheFile):
        return None, False
    stndata=None
    iscurrent=False
    try:
        import sqlite3
        conn=sqlite3.connect(_cacheFile)
        c=conn.cursor()
        dateoffset='-'+str(_cacheExpiry)+' hours'
        c.execute(""" select 
                    json, 
                    CASE WHEN cachedate >= datetime('now',?) THEN 'Y' else 'N' END as iscurrent 
                  from gdb_json where code=?""",
                  (dateoffset,code.upper()))
        for row in c:
            iscurrent=row[1] == 'Y'
            stndata=row[0]
            break
        conn.commit()
    except:
        pass
    return stndata, iscurrent

def _saveToFileCache( code, stndata ):
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
                  values (?,datetime('now'),?)
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
    
    If GDB.setCached has been called or if cache == "file" then a persistent file cache is used.
    '''
    if not re.match(r'^\w{4}$',code):
        raise ValueError(code+' is not a valid geodetic code')
    code=code.upper()
    if cache == 'file':
        global _useFileCache
        if not _useFileCache:
            setCached()
        cache=True

    if cache and code in _cache:
        stn=_cache[code]
    else:
        url=_gdburl.replace('{code}',code)
        stndata=None
        try:
            stndata, iscurrent=_getFromFileCache(code)
            if stndata is None or not iscurrent:
                stndata=urllib2.urlopen(url).read()
                _saveToFileCache(code,stndata)
        except Exception as e:
            if stndata is None:
                raise RuntimeError("Cannot connect to geodetic database: "+e.message)
        if stndata is not None:
            stn=json.loads(stndata,object_hook=_json_object_hook)
        if cache:
            _cache[code]=stn
    if stn is None:
        raise ValueError(code+' is not an existing geodetic mark')
    return stn
    
