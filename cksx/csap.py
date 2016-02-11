#!/usr/bin/env python3
from queue import Queue
from threading import Thread
from time import sleep
import requests
import json
import sys
import pandas as pd
from io import StringIO


class SAPTableTask:
    """
    Defines the chunks of a SAPTable that will be downloaded.
    """
    def __init__(self, table, ri, n, keep=False):
        """
        Create a task
        :param table: SAPTable object to which the task belongs
        :param ri: Starting row for chunk
        :param n: Number of rows in chunk
        :param keep: Boolean, keep the extracted data in memory in the task
        """
        self.table = table
        self.ri = ri
        self.n = n
        self.timestamp = None
        self.status = (None, 'Created')  # (node, status)
        self.count = 0
        self.keep = keep
        self.data = None

    def execute(self, node, sqlalchemy_cnxnstr=None):
        """
        Execute the task on its table.
        :param node: URL of the SAP node on which to execute
        :param sqlalchemy_cnxnstr: (optional) SQLAlchemy connection string to target database
        :return: Outcome of task execution, boolean
        """
        # Build and send SAP node request to read
        headers = {'Content-Type': 'application/json'}
        data = {'cnxn_details': self.table.cnxn_details,
                'table_name': self.table.table_name,
                'ri': self.ri,
                'n': self.n,
                'where': self.table.where,
                'vchunks': self.table.vchunks,
                'sqlalchemy_cnxnstr': sqlalchemy_cnxnstr,
                'keep': self.keep}
        if self.table.output_tablename is not None:
            data['output_tablename'] = self.table.output_tablename
        url = node + self.table.SAPNODE_ROUTE

        try:
            res = requests.post(url=url, data=json.dumps(data))
            msg = res.json()
            self.status = (node, msg['STATUS'])
            self.count = int(msg['COUNT'])
            self.timestamp = msg['TIMESTAMP']
            if self.keep:
                self.data = pd.read_csv(StringIO(msg['DATA']))
        except Exception as e:
            print('Error at task %s(%d - %d):\n%s'
                  % (self.table.table_name, self.ri, self.ri + self.n, str(e)))
            self.status = (node, 'FAIL')

        if self.status[1] == 'OK':
            if self.count + self.ri >= self.table.rmax or self.count < self.n:
                self.table.complete = True
            self.table.count += self.count
            self.table.tasks.append(self)
            return True
        self.table.tasks.append(self)
        return False


class SAPTable:
    """

    """
    PREREQ_MISSING = 0
    PREREQ_PENDING = 1
    PREREQ_SUCCESS = 2
    SAPNODE_ROUTE = '/read'  # POST
    SAPNODE_META_ROUTE = '/meta'   # GET
    SAPNODE_CONNECTION_ROUTE = '/info'  # POST, GET
    SAP_BUFFER_SIZE = 400

    def __init__(self, system, auth, table_name, fields=None, r0=0, rmax=1000,
                 chunksize=10000, where='', output_tablename=None, keep=False,
                 dtypes=None):
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
        self.dtypes = dtypes
        self.output_tablename = output_tablename
        self.keep = keep

        self.complete = False
        self.vchunks = None
        self.meta = None
        self.tasks = []

    def connection_test(self, node):
        """
        Checks the connection to SAP via the node.
        :param node: URL of the SAP node
        :return: Connection parameters or error detail
        """
        res = requests.post(url=node + self.SAPNODE_CONNECTION_ROUTE,
                            data=json.dumps({'cnxn_details':
                                             self.cnxn_details}))
        return json.loads(res.text)

    def get_next_task(self):
        """
        Get the next TableTask, ie. the next row-wise chunk
        Useful for chunking / looping through rows.
        Updates the ri_next attribute of the table, the start of the next chunk.
        :return: TableTask for the next batch
        """
        if self.ri_next < self.rmax:
            t = SAPTableTask(self, self.ri_next, self.chunksize, keep=self.keep)
            self.ri_next += self.chunksize
            return t
        else:
            return None

    def get_task(self, ri=0, n=1000000):
        """
        Prepare a task to download.
        :param ri: Starting row
        :param n: Number of rows
        :return: TableTask
        """
        return SAPTableTask(self, ri, n, keep=self.keep)

    def prerequisites(self, node):
        """
        Prepares the table for download, fetching metadata.
        :param node:
        :return:
        """
        while self.meta_status != self.PREREQ_SUCCESS:
            if self.meta_status == self.PREREQ_PENDING:
                # wait for metadata task to complete
                sleep(2)
            elif self.meta_status == self.PREREQ_MISSING:
                # fetch the metadata
                self.meta_status = self.PREREQ_PENDING
                self.get_meta(node)
                self.meta_status = self.PREREQ_SUCCESS

    def get_meta(self, node):
        """
        Fetches the metadata of the table from node/SAPNODE_META_ROUTE
        :param node: URL of the SAP node on which to run the job
        :return: Dataframe containing metadata
        """
        # Send request to SAP node
        res = requests.post(url=node + self.SAPNODE_META_ROUTE,
                            data=json.dumps({'cnxn_details': self.cnxn_details,
                                             'table_name': self.table_name,
                                             'fields': self.fields,
                                             'sap_buffer_size': self.SAP_BUFFER_SIZE}))

        resjson = res.json()
        self.vchunks = resjson['vchunks']
        self.meta = pd.read_csv(StringIO(resjson['meta_csv']))
        self.meta.LENG = self.meta.LENG.astype(int)
        self.meta_status = self.PREREQ_SUCCESS
        return self.meta

    def get_downloaded_dataframe(self, task_idxs=None, drop_duplicates=True):
        """
        Concatenate tasks' data into a single dataframe
        :param task_idxs: (optional) The tasks to concatenate
        :param drop_duplicates: Drop duplicates from resulting dataframe
        :return:
        """
        if not task_idxs:
            task_idxs = range(len(self.tasks))
        dfout = pd.concat([self.tasks[idx].data for idx in task_idxs])
        dfout.reset_index(drop=True, inplace=True)
        if drop_duplicates:
            dfout.drop_duplicates(inplace=True, subset=dfout.columns[:-1])
        return dfout
