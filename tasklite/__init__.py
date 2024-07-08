import sqlite3
import time
import datetime
import ctypes
import uuid
import traceback

from typing import Callable, Any
from multiprocessing import Manager, Process, Value


_DEFAULT_DB_URL = 'tasklite.db'
_SQL_SCHEMAS = {
    'message': """(
            id TEXT PRIMARY KEY,
            name TEXT,
            description TEXT,
            dt_created TEXT,
            queue TEXT,
            args TEXT,
            kwargs TEXT
        )
        """,
    'queue': """(
            id TEXT PRIMARY KEY,
            name TEXT,
            queue TEXT,
            args TEXT,
            kwargs TEXT
        )
        """,
    'results': """(
            id TEXT PRIMARY KEY,
            name TEXT,
            status TEXT,
            result TEXT,
            dt_created TEXT,
            dt_done TEXT,
            execution_time TEXT,
            error TEXT,
            traceback TEXT
        )
        """,
}


class Database:
    def __init__(self, db_url: str, timeout: int = 10):
        self.db_url = db_url
        self.connection = sqlite3.connect(self.db_url, timeout=timeout)
        self.cursor = self.connection.cursor()
        self._init_db()

    def _init_db(self):
        for table, schema in _SQL_SCHEMAS.items():
            sql = f'CREATE TABLE IF NOT EXISTS {table} {schema}'
            self.cursor.execute(sql)
        self.connection.commit()

    def _insert_message(
        self,
        task_id,
        name,
        description,
        dt_created,
        queue,
        args,
        kwargs,
    ):
        self.cursor.execute(
            """
            INSERT INTO message (id, name, description, dt_created, queue, args, kwargs)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (task_id, name, description, dt_created, queue, args, kwargs),
        )
        self.connection.commit()

    def insert_queue(
        self,
        task_id,
        name,
        queue,
        description,
        dt_created,
        args,
        kwargs,
    ):
        self._insert_message(
            task_id, name, description, dt_created, queue, args, kwargs
        )
        self.cursor.execute(
            """
            INSERT INTO queue (id, name, queue, args, kwargs)
            VALUES (?, ?, ?, ?, ?)
            """,
            (task_id, name, queue, args, kwargs),
        )
        self.connection.commit()

    def insert_result(
        self,
        task_id: str,
        task_name: str,
        status: str,
        result: Any,
        dt_started: datetime.datetime,
        dt_done: datetime.datetime,
        execution_time: float,
        error: str | None,
        tb: str | None,
    ):
        self.cursor.execute(
            """
            INSERT INTO results 
            (id, name, status, result, dt_created, dt_done, execution_time, error, traceback)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task_id,
                task_name,
                status,
                str(result),
                dt_started.strftime('%Y-%m-%d %H:%M:%S'),
                dt_done.strftime('%Y-%m-%d %H:%M:%S'),
                execution_time,
                error,
                tb,
            ),
        )
        self.connection.commit()

    def delete_from_queue(self, task_id):
        self.cursor.execute('DELETE FROM queue WHERE id = ?', (task_id,))
        self.connection.commit()

    def fetch_queue(self):
        self.cursor.execute('SELECT * FROM queue')
        return self.cursor.fetchall()


class Task:
    def __init__(
        self,
        func: Callable,
        db_url: str = _DEFAULT_DB_URL,
        name: str | None = None,
        description: str | None = None,
        queue: int = 1,
    ):
        self.func = func
        self.name = name or func.__name__
        self.db_url = db_url
        self.description = description
        self.priority_queue = queue

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def queue(self, *args, **kwargs):
        db = Database(self.db_url)
        task_id = str(uuid.uuid4())
        dt_created = time.strftime('%Y-%m-%d %H:%M:%S')
        db.insert_queue(
            task_id=task_id,
            name=self.name,
            queue=self.priority_queue,
            description=self.description,
            dt_created=dt_created,
            args=None if not args else str(args),
            kwargs=None if not kwargs else str(kwargs),
        )


class Worker:
    def __init__(self, number: int):
        self.number = number
        self.available = Value(ctypes.c_bool, True)

        print(f'worker {self.number} ok')

    def __str__(self):
        return f'worker{self.number}'

    def __repr__(self):
        return f'worker{self.number}'

    def _execute_task(
        self,
        task_id: str,
        task_name: str,
        func: Callable,
        db_url: str,
    ):
        dt_started = datetime.datetime.now()
        db = Database(db_url)

        try:
            # result = func(*args, **kwargs) # FIXME
            result = func()
            status = 'SUCCESS'
            error = None
            tb = None
        except Exception as e:
            result = None
            status = 'FAILED'
            error = str(e)
            tb = traceback.format_exc()

        dt_done = datetime.datetime.now()

        execution_time = (dt_done - dt_started).seconds

        if status == 'SUCCESS':
            print(f'[{self}][{task_id}] {task_name} succeeded in {execution_time}s')
        else:
            print(f'[{self}][{task_id}] {task_name} failed in {execution_time}s')

        db.insert_result(
            task_id,
            task_name,
            status,
            str(result),
            dt_started,
            dt_started,
            execution_time,
            error,
            tb,
        )
        self.available.value = True

    def run_task(
        self,
        task_id: str,
        task_name: str,
        func: Callable,
        db_url: str,
        *args,
        **kwargs,
    ):
        self.available.value = False
        print(self, f'Executing task {task_name}')
        process = Process(
            target=self._execute_task,
            args=(task_id, task_name, func, db_url),
        )
        process.start()


class TaskLite:
    def __init__(
        self,
        tasks: list[Task] | None = None,
        workers: int = 4,
        db_url: str = _DEFAULT_DB_URL,
        loop_interval: float = 0.1,
    ):
        self._nworkers = workers
        self.tasks = tasks or []
        self.tasks_funcs = {}
        self.db_url = db_url
        self.db = None
        self.db = self.get_db()
        self.active_processes = []
        self.loop_interval = loop_interval

    def init(self):
        self.manager = Manager()
        self.queue = self.manager.Queue()
        self.workers = [Worker(i + 1) for i in range(self._nworkers)]

    @property
    def available_workers(self):
        return [worker for worker in self.workers if worker.available.value]

    def add_task(self, task: Callable):
        _task = Task(task, db_url=self.db_url)
        self.tasks.append(_task)
        self.tasks_funcs.update({_task.name: _task.func})

    def get_db(self) -> Database:
        return self.db or Database(self.db_url)

    def _get_task_func(self, task_name: str) -> Callable:
        for task in self.tasks:
            if task.name == task_name:
                return task.func
        raise ValueError(f'Task {task_name} not found in TaskLite')

    def queue_task(
        self,
        name: str,
        description: str | None = None,
        priority_queue: int = 1,
        *args,
        **kwargs,
    ):
        task_id = str(uuid.uuid4())
        dt_created = time.strftime('%Y-%m-%d %H:%M:%S')
        db = self.get_db()
        db.insert_queue(
            task_id=task_id,
            name=name,
            queue=priority_queue,
            description=description,
            dt_created=dt_created,
            args=None if not args else str(args),
            kwargs=None if not kwargs else str(kwargs),
        )

    def run_pending(self):
        db = self.get_db()
        available_workers = self.available_workers
        if available_workers:
            queue = db.fetch_queue()
            tasks_to_execute = queue[: len(available_workers)]
            if tasks_to_execute:
                for task, worker in zip(tasks_to_execute, available_workers):
                    task_id, name, _, args, kwargs = task
                    func = self._get_task_func(name)
                    worker.run_task(
                        task_id=task_id,
                        task_name=name,
                        func=func,
                        db_url=self.db_url,
                        args=args,
                        kwargs=kwargs,
                    )

                    db.delete_from_queue(task_id)
        else:
            ...

    def flush_db(self):
        db = self.get_db()
        db.cursor.execute('DELETE FROM message')
        db.cursor.execute('DELETE FROM queue')
        db.cursor.execute('DELETE FROM results')
        db.connection.commit()

    def run(self):
        self.init()

        while True:
            self.run_pending()
            time.sleep(self.loop_interval)
