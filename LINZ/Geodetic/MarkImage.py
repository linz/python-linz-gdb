
import re
from datetime import datetime
from collections import namedtuple

class MarkImageError(RuntimeError):
    pass

nametypes= [
    ('SITE', re.compile(r'^(?P<code>[A-Z0-9]{4})(?P<Y2>\d\d)P(?P<id>\d)\.(?P<format>jpg)$',re.I)),
    ('DIAG', re.compile(r'^(?P<code>[A-Z0-9]{4})(?P<Y2>\d\d)(?P<M>0[1-9]|1[012])(?P<id>\d)?\.(?P<format>jpg|tif)$',re.I)),
    ('RELB', re.compile(r'^(?P<code>[A-Z0-9]{4})(?P<Y2>\d\d)R(?P<id>\d)\.(?P<format>tif)$',re.I)),
    ('ACDP', re.compile(r'^(?P<code>[A-Z0-9]{4})(?P<Y2>\d\d)AD(?P<id>\d)\.(?P<format>png)$',re.I)),
    ('BCDP', re.compile(r'^(?P<code>[A-Z0-9]{4})(?P<Y2>\d\d)BD(?P<id>\d)\.(?P<format>png)$',re.I)),
    ]

basepath="marks"

ImageMetadata=namedtuple('ImageMetadata','filename normalized geodetic_code type format date filesize lastmod')

def ParseFilename( filename, filesize=None, lastmod=None ):
    for type, re in nametypes:
        m=re.match(filename)
        if not m:
            continue
        md=m.groupdict()
        geodetic_code=md.get('code').upper()
        year=int(md.get('Y2'))
        year=year+(2000 if year < 50 else 1900)
        month=int(md.get('M','06'))
        date=datetime(year,month,1,0,0,0)
        format=md.get('format').upper()
        if format == 'TIF':
            format='TIFF'
        elif format == 'JPG':
            format='JPEG'
        normalized=filename[:-4].upper()+filename[-4:].lower()
        return ImageMetadata(filename,normalized,geodetic_code,type,format,date,filesize,lastmod)
    raise MarkImageError('Invalid mark image filename '+filename)

def ImagePath( filename ):
    code=filename[:4].upper()
    return "/".join((basepath,code[:1],code[:2],code[:3],code[:4],filename))
