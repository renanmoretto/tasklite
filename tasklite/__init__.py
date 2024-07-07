import sqlite3
import time
from multiprocessing import Manager, Process
from typing import Callable, List
import uuid


class Task:
    def __init__(self):
        ...


class TaskLite:
    _schemas = {
        'message': """
            CREATE TABLE IF NOT EXISTS message (
                id TEXT PRIMARY KEY,
                name TEXT,
                description TEXT,
                dt_created TEXT,
                queue TEXT,
                args TEXT,
                kwargs TEXT
            )
            """,
        'queue': """
            CREATE TABLE IF NOT EXISTS queue (
                id TEXT PRIMARY KEY,
                name TEXT,
                queue TEXT,
                args TEXT,
                kwargs TEXT
            )
            """,
        'result': """
            CREATE TABLE IF NOT EXISTS results (
                id TEXT PRIMARY KEY,
                name TEXT,
                status TEXT,
                result TEXT,
                dt_created TEXT,
                dt_done TEXT,
                error TEXT,
                traceback TEXT
            )
            """,
    }

    def __init__(
        self, tasks: List[Callable], workers: int = 4, db_url: str = 'tasklite.db'
    ):
        self.tasks = {func.__name__: func for func in tasks}
        self.nworkers = workers
        self.db_url = db_url
        self.connection = sqlite3.connect(self.db_url, check_same_thread=False)
        self.cursor = self.connection.cursor()
        self._init_db()

    def init(self):
        self.manager = Manager()
        self.queue = self.manager.Queue()
        self.available_workers = self.manager.list([True] * self.nworkers)

    def _init_db(self):
        for sql in self._schemas.values():
            self.cursor.execute(sql)
        self.connection.commit()

    def _get_db_queue(self):
        self.cursor.execute('SELECT * FROM queue')
        return self.cursor.fetchall()

    def queue_task(self, name: str, description=None, queue: int = 1, *args, **kwargs):
        task_id = str(uuid.uuid4())
        dt_created = time.strftime('%Y-%m-%d %H:%M:%S')
        self.cursor.execute(
            """
            INSERT INTO message (id, name, description, dt_created, queue, args, kwargs)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            (
                task_id,
                name,
                description,
                dt_created,
                name,
                None if not args else str(args),
                None if not args else str(kwargs),
            ),
        )
        self.cursor.execute(
            """
            INSERT INTO queue (id, name, queue, args, kwargs)
            VALUES (?, ?, ?, ?, ?)
        """,
            (
                task_id,
                name,
                queue,
                None if not args else str(args),
                None if not args else str(kwargs),
            ),
        )
        self.connection.commit()

    def run_pending(self):
        if any(self.available_workers):
            task = self._get_db_queue()
            if task:
                task_id, name, _, args, kwargs = task[0]
                func = self.tasks[name]
                worker_index = self.available_workers.index(True)
                self.available_workers[worker_index] = False
                process = Process(
                    target=_execute_task,
                    args=(task_id, func, args, kwargs, self.db_url),
                )
                process.start()
                # process.join()
                self.available_workers[worker_index] = True
                self.cursor.execute('DELETE FROM queue WHERE id = ?', (task_id,))
                self.connection.commit()
        else:
            print('Waiting for available workers')

    def flush_db(self):
        self.cursor.execute('DELETE FROM message')
        self.cursor.execute('DELETE FROM queue')
        self.cursor.execute('DELETE FROM results')
        self.connection.commit()


def _execute_task(task_id, func, args, kwargs, db_url):
    dt_started = time.strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(db_url)
    cursor = conn.cursor()
    try:
        result = func(args, **kwargs)
        status = 'success'
        traceback = None
    except Exception as e:
        result = None
        status = 'failed'
        error = str(e)
        traceback = str(e)
    dt_done = time.strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute(
        """
        INSERT INTO results (id, name, status, result, dt_created, dt_done, error, traceback)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            task_id,
            func.__name__,
            status,
            str(result),
            dt_started,
            dt_done,
            error,
            traceback,
        ),
    )
    conn.commit()
    conn.close()
