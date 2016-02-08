#!/usr/bin/env python2

from pyrfc import Connection as SAP_cnxn
import pandas as pd
import sqlite3
import sqlalchemy
import json
from datetime import datetime

dd03l_fields = ['FIELDNAME', 'AS4LOCAL', 'AS4VERS', 'POSITION',
                'KEYFLAG', 'ROLLNAME', 'CHECKTABLE', 'INTTYPE',
                'INTLEN', 'LENG']


def gFIELDS(fields):
    return [{'FIELDNAME': fi} for fi in fields]


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


def read(cnxn_details, table_name, vchunks, ri, n, where,
         sqlalchemy_cnxnstr='sqlite:////home/cks/db.sqlite',
         output_tablename=None, tag='', keep=False):
    with SAP_cnxn(**cnxn_details) as cnxn:
        data = None
        fields = None
        for vchunk in vchunks:
            chunk_response = cnxn.call(
                'BBP_RFC_READ_TABLE',
                QUERY_TABLE=table_name,
                DELIMITER='|',
                OPTIONS=[{'TEXT': where}],
                FIELDS=gFIELDS(vchunk),
                ROWCOUNT=n,
                ROWSKIPS=ri,
                NO_DATA=''
            )
            if chunk_response['DATA'] is None:
                break
            chunk = [map(unicode.strip, x['WA'].split('|'))
                     for x in chunk_response['DATA']]
            if data is None:
                data = chunk
                fields = vchunk
            else:
                data = [data[idx] + chunkrow
                        for idx, chunkrow in enumerate(chunk)]
                fields += vchunk

        timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        response = {'data': data, 'columns': fields}
        df = pd.DataFrame(**response)
        df['TIMESTAMP'] = timestamp
        count = len(df)

        # write to sqlite
        jout = {'STATUS': 'FAIL', 'TIMESTAMP': timestamp, 'COUNT': count}
        if sqlalchemy_cnxnstr is not None:
            engine = sqlalchemy.create_engine(sqlalchemy_cnxnstr)
            output_tablename = output_tablename if output_tablename else 'csap_' + table_name + tag
            df.to_sql(output_tablename,
                      engine, if_exists='append',
                      chunksize=50000, index=False)
            jout['STATUS'] = 'OK'
        if keep:
            jout['DATA'] = df.to_json()
            jout['STATUS'] = 'OK'
        return json.dumps(jout)
