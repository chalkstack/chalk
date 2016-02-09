#!/usr/bin/env python2

from pyrfc import Connection as SAP_cnxn
import pandas as pd
import sqlite3
import sqlalchemy
import json
from datetime import datetime


def info(cnxn_details):
    try:
        cnxn = SAP_cnxn(**cnxn_details)
        cnxn.close()
        cnxn_info = cnxn.get_connection_attributes()
        return json.dumps({'status': 'OK', 'data': cnxn_info})
    except Exception, e:
        return json.dumps({'status': 'fail', 'data': str(e)})


def get_meta(cnxn_details, table_name):
    with SAP_cnxn(**cnxn_details) as cnxn:
        meta_result = cnxn.call('BBP_RFC_READ_TABLE',
                                QUERY_TABLE='DD03L',
                                DELIMITER='|',
                                OPTIONS=[{'TEXT': "TABNAME = '%s'"
                                         % table_name}],
                                FIELDS=gFIELDS(dd03l_fields))
        data = [map(unicode.strip, x['WA'].split('|'))
                for x in meta_result['DATA']]
        columns = [x['FIELDNAME'] for x in meta_result['FIELDS']]
    return json.dumps({'data': data, 'columns': columns})



