#!/usr/bin/env python2
from queue import Queue
from threading import Thread
from time import sleep
import requests
import json
import sys
import pandas as pd


class TableTask():
    def __init__(self, table, ri, n):
        self.table = table
        self.ri = ri
        self.n = n
        self.timestamp = None
        self.status = 0  # -1: fail, 0: unprocessed, 1: successful
        self.count = 0

    def execute(self, engine, sqlalchemy_cnxnstr=None, notags=False):
        headers = {'Content-Type': 'application/json'}
        data = {'cnxn_details': self.table.cnxn_details,
                'table_name': self.table.table_name,
                'ri': self.ri,
                'n': self.n,
                'where': self.table.where,
                'vchunks': self.table.vchunks,
                'sqlalchemy_cnxnstr': sqlalchemy_cnxnstr,
                'tag': '' if notags else engine.rsplit(':', 1)[1],
                'keep': self.table.keep}
        if self.table.output_tablename is not None:
            data['output_tablename'] = self.table.output_tablename
        url = engine + self.table.SAPNODE_ROUTE
        res = requests.post(url=url, data=json.dumps(data))
        try:
            msg = res.json()
            self.status = msg['STATUS']
            self.count = int(msg['COUNT'])
            self.table.count += self.count
            self.timestamp = msg['TIMESTAMP']
            if 'DATA' in msg:
                self.table.data.append(msg['DATA'])
        except ValueError:
            print('%s(%d - %d)value error caught trying to parse task \
                   response message.'
                  % (self.table.table_name, self.ri, self.ri + self.n))
            print(res)
            print(res.text)
            self.status = 'FAIL'
            self.count = 0
            self.table.count += 0

        if self.count + self.ri >= self.table.rmax or self.count < self.n:
            self.table.complete = True
        if self.status == 'OK':
            return True
        return False


class SAPTable():
    PREREQ_MISSING = 0
    PREREQ_PENDING = 1
    PREREQ_SUCCESS = 2
    SAPNODE_ROUTE = '/read'  # POST
    SAPNODE_META_ROUTE = '/meta'   # GET
    SAPNODE_CONNECTION_ROUTE = '/info'  # POST, GET
    SAP_BUFFER_SIZE = 400

    def __init__(self, system, auth, table_name, fields=None, r0=0, rmax=1000,
                 chunksize=10000, where='', output_tablename=None, keep=False):
        self.table_name = table_name
        self.fields = fields
        self.cnxn_details = {'ashost': system[0],
                             'sysnr': system[1],
                             'client': system[2],
                             'user': auth[0],
                             'passwd': auth[1]}
        self.r0 = r0
        self.ri_next = r0
        self.rmax = rmax
        self.chunksize = chunksize
        self.meta_status = 0
        self.where = where
        self.count = 0
        self.complete = False
        self.vchunks = None
        self.data = []
        self.output_tablename = output_tablename
        self.keep = keep

    def connection_test(self, engine):
        res = requests.post(url=engine + self.SAPNODE_CONNECTION_ROUTE,
                            data=json.dumps({'cnxn_details':
                                             self.cnxn_details}))
        return json.loads(res.text)

    def seed_tasks(self, max_threads=1, reset=True):
        if reset:
            self.ri_next = self.r0
        for i in range(max_threads):
            next_task = self.get_next_task()
            if next_task is not None:
                yield next_task
            else:
                break

    def get_next_task(self):
        if self.ri_next < self.rmax:
            t = TableTask(self, self.ri_next, self.chunksize)
            self.ri_next += self.chunksize
            return t
        else:
            return None

    def prerequisites(self, engine, timeout=None):
        js_meta_params = ['cnxn_details', 'table_name']
        while self.meta_status != self.PREREQ_SUCCESS:
            if self.meta_status == self.PREREQ_PENDING:
                # wait for metadata task to complete
                sleep(1)
            elif self.meta_status == self.PREREQ_MISSING:
                # fetch the metadata
                self.meta_status = self.PREREQ_PENDING
                self.get_meta(engine)
                self.meta_status = self.PREREQ_SUCCESS

    def get_meta(self, engine):
        self.meta_status = self.PREREQ_PENDING
        res = requests.post(url=engine + self.SAPNODE_META_ROUTE,
                            data=json.dumps({'cnxn_details': self.cnxn_details,
                                             'table_name': self.table_name}))
        self.meta = pd.DataFrame(**res.json()).set_index('FIELDNAME')
        self.meta.LENG = self.meta.LENG.astype(int)
        self.meta_status = self.PREREQ_SUCCESS
        try:
            self.meta.drop('.INCLUDE', inplace=True)
        except ValueError:
            pass

        if self.fields is None:
            self.fields = self.meta.index.tolist()

        vchunks = []
        chunksum = 0
        chunk = []
        for field in self.fields:
            length = self.meta.loc[field.upper(), 'LENG']
            chunksum += length
            if chunksum > self.SAP_BUFFER_SIZE:
                vchunks.append(chunk)
                chunksum = 0
                chunk = [field]
            else:
                chunk.append(field)
        if chunk != []:
            vchunks.append(chunk)
        self.vchunks = vchunks

    def get_downloaded_dataframe(self):
        dfout = pd.concat([pd.read_json(x, dtype=str)[self.fields] for x in self.data])\
                .reset_index(drop=True)
        return dfout

class Worker(Thread):
    def __init__(self, queue, engine):
        Thread.__init__(self)
        self.queue = queue
        self.engine = engine
        self.Daemon = True

        self.status = True
        self.processed = []
        self.sqlalchemy_cnxnstr = queue.sqlalchemy_cnxnstr

    def run(self):
        while True:
            # Get a task from the queue
            task = self.queue.get()

            # check the table prerequisites are satisfied
            task.table.prerequisites(self.engine)

            # execute the task
            if task.execute(self.engine, self.sqlalchemy_cnxnstr,
                            notags=self.queue.notags) is False:
                self.status = False
            self.processed.append(task)

            # If there are more tasks for the table put one onto the queue
            # before completing.
            next_task = task.table.get_next_task()
            if next_task is not None:
                self.queue.put(next_task, block=False)

            # Task is complete
            self.queue.task_done()


class Extractor(Queue):
    default_engines = ['http://172.17.42.1:5101',
                       'http://172.17.42.1:5102',
                       'http://172.17.42.1:5103',
                       'http://172.17.42.1:5104']

    def __init__(self, engines=None, start=True,
                 sqlalchemy_cnxnstr='sqlite:///db.sqlite', notags=False):
        Queue.__init__(self)
        self.engines = engines or self.default_engines
        self.tables = []
        self.sqlalchemy_cnxnstr = sqlalchemy_cnxnstr
        self.notags = notags
        if start:
            self.start()

    def start(self):
        self.engines[:] = [x for x in self.engines if self.cnxn_test(x)]
        self.workers = [Worker(self, engine) for engine in self.engines]
        for worker in self.workers:
            worker.start()

    def cnxn_test(self, engine):
        try:
            res = requests.get(engine, timeout=(1.0, 1.0))
            print('%s is %s' % (engine, res.text))
            return res.text == 'UP'
        except (requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectTimeout,
                requests.ConnectionError):
            print('%s is DOWN' % engine)
            return False

    def extract(self, table, parallelism=1):
        self.tables.append(table)
        for task in table.seed_tasks(parallelism):
            self.put(task, block=False)

    def blocking_status(self):
        while not all([t.complete for t in self.tables]):
            sys.stdout.write('\r')
            for t in self.tables:
                sys.stdout.write('[%s: %d / %d]\t'
                                 % (t.table_name, t.count, t.rmax))
            sys.stdout.flush()
            sleep(0.1)

        sys.stdout.write('\r')
        for t in self.tables:
            sys.stdout.write('[%s: %d / %d]\t'
                             % (t.table_name, t.count, t.rmax))
        sys.stdout.write('Done.')
        sys.stdout.flush()
