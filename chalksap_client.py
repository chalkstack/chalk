import json, requests, pandas as pd

def build_request_full_table(table, cred):
    js = json.dumps(
        {'host': cred['ashost'],
         'client': cred['client'],
         'sysnr': cred['sysnr'],
         'table': table,
         'row_max': 100000,
         'batch_size': 100000}
    )
    headers = {'Content-Type': 'application/json'}
    auth = (cred['user'], cred['passwd'])
    return {'url': 'http://127.0.0.1:5000/chalksap/read_small_table',
            'auth': auth,
            'headers': headers,
            'data':js}

def build_request_table(table, cred, fields=None, where='',
                        server_url='http://172.17.42.1',
                        server_port='5100',
                        row_max=1000000,
                        batch_size=100000,
                        **kwargs):
    djs = {'host': cred['ashost'],
         'client': cred['client'],
         'sysnr': cred['sysnr'],
         'table': table,
         'fields': fields,
         'where': where,
         'row_max': row_max,
         'batch_size': batch_size}
    djs.update(kwargs)
    headers = {'Content-Type': 'application/json'}
    auth = (cred['user'], cred['passwd'])
    return {'url': '%s:%s/chalksap/read_small_table' % (server_url,server_port),
            'auth': auth,
            'headers': headers,
            'data': json.dumps(djs)}

def response_to_df(res):
    return pd.DataFrame(
        data=[x.split('|') for x in res.json()['DATA']],
        columns=res.json()['FIELDNAME']
    )
