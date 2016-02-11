#!/usr/bin/env python3
"""

"""

class Worker(Thread):
    """
    Worker class for executing SAPTableTasks in threads.
    Subclass Thread to create workers that add to a common queue after
    completing their task.
    """
    def __init__(self, queue, node):
        """
        Initialise a Worker object.
        :param queue: Extractor object (contains the queue that the worker
                      gets and puts).
        :param node: The URL of the SAP node which the worker will query.
        :return:
        """
        Thread.__init__(self)
        self.queue = queue
        self.node = node
        self.Daemon = True
        self.status = True
        self.sqlalchemy_cnxnstr = queue.sqlalchemy_cnxnstr

    def run(self):
        """
        Overloads Thread.run, defining the processing flow of each Worker Thread.
        (1) Get a task from the Extractor Queue
        (2) Check prerequisites are met for the task
        (3) Execute the task
        (4) Get the next task, or look to the next table in the Extractor
            if the current table is complete.
        """
        while True:
            # Get a task from the queue
            task = self.queue.get()

            # check the table prerequisites are satisfied
            task.table.prerequisites(self.node)

            # execute the task
            if task.execute(self.node, self.sqlalchemy_cnxnstr) is False:
                self.status = False

            # If there are more tasks for the table put one onto the queue
            # before completing.
            next_task = task.table.get_next_task()
            if next_task is not None:
                self.queue.put(next_task, block=False)

            # If the table is complete, seed this worker with the next
            # incomplete table's task.
            else:
                next_tables_task = self.queue.get_next_incomplete_table_task()
                if next_tables_task is not None:
                    self.queue.put(next_task, block=False)

            # Task is complete
            self.queue.task_done()


class Extractor(Queue):
    """
    Extractor subclassing Queue for threading SAPTable download tasks.
    The Extractor seeds an initial queue of SAPTableTasks from the SAPTables
    attached to it. Workers execute the initial queue, and put to the queue
    when each task completes, until all SAPTables are complete.
    """
    def __init__(self, nodes=None, start=True,
                 sqlalchemy_cnxnstr='sqlite:///db.sqlite'):
        Queue.__init__(self)
        self.nodes = nodes or self.default_nodes
        self.tables = []
        self.sqlalchemy_cnxnstr = sqlalchemy_cnxnstr
        self.workers = None
        if start:
            self.start()

    def start(self):
        """
        Starts the workers from the initial list of SAP nodes provided.
        Only starts workers that pass cnxn_test.
        """
        self.nodes[:] = [x for x in self.nodes if self.cnxn_test(x)]
        self.workers = [Worker(self, node) for node in self.nodes]
        for worker in self.workers:
            worker.start()

    def cnxn_test(self, node):
        """
        Checks that the SAP node is up.
        :param node: URL of the SAP node.
        :return: Result, boolean
        """
        try:
            res = requests.get(node, timeout=(5.0, 5.0))
            print('%s is %s' % (node, res.text))
            return res.text == 'UP'
        except (requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectTimeout,
                requests.ConnectionError):
            print('%s is DOWN' % node)
            return False

    def extract(self, table, parallelism=1):
        """
        Push a SAPTable <table> onto the queue, seeding <parallelism> tasks.
        :param table: SAPTable object to be extracted.
        :param parallelism: Number of tasks to put into initial queue.
        """
        for idx in range(parallelism):
            next_task = table.get_next_task()
            if next_task is not None:
                self.put(next_task, block=False)
            else:
                break
        self.tables.append(table)

    def blocking_status(self):
        """
        Block workers and wait for all tables to finish.
        Writes progress to stdout.
        """
        while not all([t.complete for t in self.tables]):
            sys.stdout.write('\r')
            for t in self.tables:
                sys.stdout.write('[%s: %d / %d]\t'
                                 % (t.table_name, t.count, t.rmax))
            sys.stdout.flush()
            sleep(0.5)

        sys.stdout.write('\r')
        for t in self.tables:
            out_of = t.count if t.complete else t.rmax
            sys.stdout.write('[%s: %d / %d]\t'
                             % (t.table_name, t.count, out_of))
        sys.stdout.write('Done.')
        sys.stdout.flush()

    def get_next_incomplete_table_task(self):
        """
        Returns a task from the first incomplete table in the payload.
        """
        for table in self.tables:
            if not table.complete:
                return table.get_next_task()
        return None
