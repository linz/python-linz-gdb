
import re
from datetime import datetime

class MarkImageError(RuntimeError):
    pass

nametypes= [
    ('SITE', re.compile(r'^(?P<code>[A-Z0-9]{4})(?P<Y2>\d\d)P(?P<id>\d?)\.(?P<format>jpg)$',re.I)),
    ('DIAG', re.compile(r'^(?P<code>[A-Z0-9]{4})(?P<Y2>\d\d)(?P<M>0[1-9]|1[012])(?P<id>\d?)\.(?P<format>jpg|tif)$',re.I)),
    ('RELB', re.compile(r'^(?P<code>[A-Z0-9]{4})(?P<Y2>\d\d)R(?P<id>\d?)\.(?P<format>tif)$',re.I)),
    ('ACDP', re.compile(r'^(?P<code>[A-Z0-9]{4})(?P<Y2>\d\d)AD(?P<id>\d?)\.(?P<format>png)$',re.I)),
    ('BCDP', re.compile(r'^(?P<code>[A-Z0-9]{4})(?P<Y2>\d\d)BD(?P<id>\d?)\.(?P<format>png)$',re.I)),
    ]

basepath="marks"

class MarkImage( object ):

    def __init__( self, filename, filesize=None, lastmod=None ):
        self.filename=filename
        self.filesize=filesize
        self.modification_date=lastmod
        self._meta=None

    @property
    def geodetic_code(self):
        return self._get_meta()['geodetic_code']

    @property
    def image_type(self):
        return self._get_meta()['image_type']

    @property
    def format(self):
        return self._get_meta()['image_format']

    @property
    def image_date(self):
        return self._get_meta()['image_date']

    def _get_meta( self ):
        if self._meta is not None:
            return self._meta
        filename=self.filename
        for image_type, re in nametypes:
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
            if normalized !=filename:
                raise MarkImageError('Incorrect image filename capitalization {0}=>{1}'
                                     .format(filename,normalized))
            self._meta={
                'geodetic_code': geodetic_code,
                'image_type': image_type,
                'image_format': format,
                'image_date': date
                 }
            return self._meta
        raise MarkImageError('Invalid mark image filename '+filename)

    def validate( self ):
        '''
        Raise an error if the filename is not valid
        '''
        self._get_meta()

    @property
    def image_path( self ):
        filename=self.filename
        code=filename[:4].upper()
        return "/".join((basepath,code[:1],code[:2],code[:3],code[:4],filename))
