from pyrfc import Connection as SAP_cnxn
import pandas as pd, sqlite3, sqlalchemy
import json
from datetime import datetime

def gFIELDS(fields):
    return [{'FIELDNAME': fi} for fi in fields]

dd03l_fields = ['FIELDNAME','AS4LOCAL','AS4VERS','POSITION',
                'KEYFLAG','ROLLNAME','CHECKTABLE','INTTYPE','INTLEN',
                'LENG']

def get_meta(cnxn_details, table_name):
    with SAP_cnxn(**cnxn_details) as cnxn:
        meta_result = cnxn.call('BBP_RFC_READ_TABLE',
                           QUERY_TABLE='DD03L',
                           DELIMITER='|',
                           OPTIONS = [{'TEXT':"TABNAME = '%s'" % table_name}],
                           FIELDS=gFIELDS(dd03l_fields))
        data = [map(unicode.strip, x['WA'].split('|')) for x in meta_result['DATA']]
        columns = [x['FIELDNAME'] for x in meta_result['FIELDS']]
        return json.dumps({'data': data, 'columns': columns})

def read(cnxn_details, table_name, vchunks, ri, n, where, sqlalchemy_cnxnstr='sqlite:////home/cks/db.sqlite', tag=''):
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
            if chunk_response['DATA'] == None:
                break
            chunk = [map(unicode.strip, x['WA'].split('|')) for x in chunk_response['DATA']]
            if data is None:
                data = chunk
                fields = vchunk
            else:
                data = [data[idx] + chunkrow for idx, chunkrow in enumerate(chunk)]
                fields += vchunk

        timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        response = {'data': data, 'columns': fields}
        df = pd.DataFrame(**response)
        df['TIMESTAMP'] = timestamp
        count = len(df)

        # write to sqlite
        engine = sqlalchemy.create_engine(sqlalchemy_cnxnstr)
        df.to_sql('csap_' + table_name + tag, engine, if_exists='append', chunksize=50000, index=False)

        return json.dumps({'STATUS': 'OK', 'TIMESTAMP': timestamp, 'COUNT': count})
