import sqlite3
import time
import datetime
from multiprocessing import Manager, Process, Value
import ctypes
from typing import Callable, Any
import uuid
import traceback


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
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.connection = sqlite3.connect(self.db_url, check_same_thread=False)
        self.cursor = self.connection.cursor()
        self._init_db()

    def _init_db(self):
        for table, schema in _SQL_SCHEMAS.items():
            sql = f'CREATE TABLE IF NOT EXISTS {table} {schema}'
            self.cursor.execute(sql)
        self.connection.commit()

    def insert_message(
        self, task_id, name, description, dt_created, queue, args, kwargs
    ):
        self.cursor.execute(
            """
            INSERT INTO message (id, name, description, dt_created, queue, args, kwargs)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (task_id, name, description, dt_created, queue, args, kwargs),
        )
        self.connection.commit()

    def insert_queue(self, task_id, name, queue, args, kwargs):
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
    def __init__(self, func: Callable, name: str | None = None):
        self.func = func
        self.name = name or func.__name__

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


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
        *args,
        **kwargs,
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
            args=(task_id, task_name, func, db_url, args, kwargs),
        )
        process.start()


class TaskLite:
    def __init__(
        self,
        tasks: list[Callable] | list[Task],
        workers: int = 4,
        db_url: str = 'tasklite.db',
    ):
        tasks = [Task(task) if not isinstance(task, Task) else task for task in tasks]

        self.tasks = tasks
        self.db_url = db_url
        self._nworkers = workers
        self.db = Database(self.db_url)
        self.active_processes = []

    def init(self):
        self.manager = Manager()
        self.queue = self.manager.Queue()
        self.workers = [Worker(i + 1) for i in range(self._nworkers)]

    @property
    def available_workers(self):
        return [worker for worker in self.workers if worker.available.value]

    def _get_task_func(self, task_name: str) -> Callable:
        for task in self.tasks:
            if task.name == task_name:
                return task.func
        raise ValueError(f'Task {task_name} not found in TaskLite')

    def queue_task(self, name: str, description=None, queue: int = 1, *args, **kwargs):
        task_id = str(uuid.uuid4())
        dt_created = time.strftime('%Y-%m-%d %H:%M:%S')
        self.db.insert_message(
            task_id,
            name,
            description,
            dt_created,
            name,
            None if not args else str(args),
            None if not args else str(kwargs),
        )
        self.db.insert_queue(
            task_id,
            name,
            queue,
            None if not args else str(args),
            None if not args else str(kwargs),
        )

    def run_pending(self):
        available_workers = self.available_workers
        if available_workers:
            queue = self.db.fetch_queue()
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

                    self.db.delete_from_queue(task_id)
        else:
            ...

    def flush_db(self):
        self.db.cursor.execute('DELETE FROM message')
        self.db.cursor.execute('DELETE FROM queue')
        self.db.cursor.execute('DELETE FROM results')
        self.db.connection.commit()
