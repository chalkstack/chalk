"""
Utility library for DATA* analysis and profiling.
Version: 0.1
Date:    January 2015
Author:  Jake Bouma

Copyright:  This code cannot be used, modified or re-distributed
            without permission of the author.
"""
import pandas as pd
from IPython.display import HTML
pd.set_option('display.max_rows', 10)
from jinja2 import Environment, FileSystemLoader
import io, codecs, shelve, os
import StringIO
import inspect

code_formatter = lambda x: ('<pre style="font: Courier New; line-height: 50%;">'+ \
                            (inspect.getsource(x).replace('\n','</pre><pre>')
                             if pd.notnull(x) else '') + '</pre>')

def mpl2base64(fig, classes=''):
    io = StringIO.StringIO()
    fig.savefig(io, format='png')
    stream = io.getvalue().encode('base64')
    strout = '<img class="%s" src="data:image/png;base64,%s"/>' % (classes, stream)
    return strout

def get_duplicates(df, gb):
    return pd.concat(g for _, g in df.groupby(gb) if len(g) > 1)

class Ninja():
    def __init__(self, template_dir='templates', shelf='ninja.db'):
        self.shelf = shelf
        self.jdata={}
        self.env = Environment(loader=FileSystemLoader(template_dir))
        self.template_dir = template_dir
    def update(self, dct):
        self.jdata.update(dct)
    def pull(self, key):
        return self.jdata[key]
    def __getitem__(self, key):
        return self.jdata[key]
    def __setitem__(self, key, val):
        self.jdata[key] = val
    def push(self, key, obj):
        self.jdata[key] = obj
    def push_many(self, dct):
        self.jdata.update(dct)
    def push_pd(self, key, df, **kwargs):
        self.jdata[key] = df.to_html(classes='pdtable', **kwargs)
    def push_figure(self, key, fig):
        # Encode image to png in base64
        io = StringIO();
        fig.savefig(io, format='png');
        stream = io.getvalue().encode('base64');
        self.jdata[key] = stream
    def get_context(self):
        return self.jdata
    def push_mpl(self, key, fig, classes=''):
        self.jdata[key] = mpl2base64(fig, classes='')
    def write(self, tpl_filename='default.tpl', html_filename='report.html'):
        template = self.env.get_template(tpl_filename)
        jout = template.render(self.jdata)
        with codecs.open(html_filename, "wb", encoding='utf-8') as fh:
            fh.write(jout)
    def shelve(self, shelf=None, **kwargs):
        if shelf is None:
            shelf = self.shelf
        sh = shelve.open(shelf, flag='n', **kwargs)
        sh.update(self.jdata)
        sh.close()
    def unshelve(self, shelf=None, **kwargs):
        if shelf is None:
            shelf = self.shelf
        sh = shelve.open(shelf, flag='r', **kwargs)
        self.jdata.update(dict(sh.items()))
        sh.close()
    def keys(self):
        return self.jdata.keys()
    def __str__(self):
        return 'ninja @ %s\n' % self.shelf +\
                '\t'.join(sorted(self.jdata.keys()))
    def unpack(self):
        locals().update(self.jdata)

def transform_csv(fnin, transform, fnout='file.csv', drop=True, chunksize=50000,
                  transform_kwargs=None, nrows=None, sep=','):
    chunk_sum = 0
    if drop:
        try:
            os.remove(fnout)
        except OSError:
            pass
    stats = {}  # dict to store counters through chunks
    write_header = True
    for chunk in pd.read_csv(fnin,
                             chunksize=chunksize,
                             sep=sep,
                             index_col=False,
                             dtype=str):
        if nrows is not None and chunk_sum > nrows:
            continue
        df = transform(chunk, stats, **transform_kwargs)
        df.to_csv(fnout, mode='a', index=False, header=write_header)
        write_header=False
        chunk_sum += chunksize
        print '++ %d = %d' % (chunksize, chunk_sum)
    return stats

import pandas.io.sql as psql
import pyodbc as odbc

def transform_mssql(con, q, transform=None, fnout='cached.csv', drop=True, chunksize=50000,
                  transform_kwargs=None, nrows=None):
    if drop:
        try:
            os.remove(fnout)
        except OSError:
            pass
    stats = {}  # dict to store counters through chunks
    write_header = True
    
    chunk_start = 1
    chunk_sum = 0

    while chunk_sum % chunksize == 0 and (nrows is None or chunk_start < nrows):
        query = q.format(**{'row_i': chunk_start,
                            'row_f': chunk_start + chunksize-1})
        print query
        chunk = psql.read_sql_query(query, con)
        if chunk.empty:
            break
        if transform is not None:
            chunk = transform(chunk, stats, **transform_kwargs)
        chunk.to_csv(fnout, mode='a', index=False, header=write_header, encoding="utf-8")
        write_header=False
        chunk_start += len(chunk)
        chunk_sum += len(chunk)
        print '++ %d = %d records' % (len(chunk), chunk_sum)
        
    return stats

def df_memory(df):
    """
    The approximate memory footprint of a dataframe in MB
    """
    return (df.values.nbytes + df.index.nbytes + df.columns.nbytes) * 1e-6


def pull_source_code(fi, check_module):
    try:
        fun = check_module.__getattribute__(fi)
    except AttributeError:
        return pd.Series([pd.np.NAN, pd.np.NAN],
                         ['vfn','src'])
    else:
        return pd.Series([inspect.getsource(fun), fun],
                         ['vfn','src'])

def load_csvs(files, transformation=None):
    result = '<table><tr><th>Data Key</th><th>Filename</th><th>Frame Dimensions</th><th>In-memory usage</th></tr>'
    dct={}
    for key, f in files.items():
        df = pd.read_csv(f, dtype=str)
        if transformation is not None:
            df = transformation(df)
        result += """<tr><td>{key}</td><td>{f}</td><td>{dfdim}</td><td>{mem}</td></tr>
                  """.format(**dict(key=key,
                                    f=f,
                                    dfdim='%d rows x %d columns' % (len(df), len(df.columns)),
                                    mem='%.2f MB' % df_memory(df)))
        dct[key] = df
        del df
    result += '</table>'
    return (result, dct)
