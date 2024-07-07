import sqlite3
import time
from multiprocessing import Manager, Process
from typing import Callable, List
import uuid


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
            error TEXT,
            traceback TEXT
        )
        """,
}


class Task:
    def __init__(self, func: Callable, name: str | None = None):
        self.func = func
        self.name = name or func.__name__

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


class Worker:
    def __init__(self, number: int, availability):
        self.number = number
        self.available = availability
        # self.available: bool = True

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
        dt_started = time.strftime('%Y-%m-%d %H:%M:%S')
        conn = sqlite3.connect(db_url)
        cursor = conn.cursor()

        try:
            # result = func(*args, **kwargs)
            result = func()
            status = 'SUCCESS'
            error = None
            traceback = None
        except Exception as e:
            result = None
            status = 'FAILED'
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
                task_name,
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
        # self.available = True
        self.available[self.number - 1] = True

    def run_task(
        self,
        task_id: str,
        task_name: str,
        func: Callable,
        db_url: str,
        *args,
        **kwargs,
    ):
        # self.available = False
        print(self, f'executing task {task_name}')
        self.available[self.number - 1] = False
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
        self.connection = sqlite3.connect(self.db_url, check_same_thread=False)
        self.cursor = self.connection.cursor()
        self._init_db()

    def init(self):
        self.manager = Manager()
        self.availability = self.manager.list([True] * self._nworkers)
        self.queue = self.manager.Queue()
        self.workers = [Worker(i + 1, self.availability) for i in range(self._nworkers)]

    @property
    def available_workers(self):
        return [
            worker for worker in self.workers if self.availability[worker.number - 1]
        ]

    def _init_db(self):
        for table, schema in _SQL_SCHEMAS.items():
            sql = f'CREATE TABLE IF NOT EXISTS {table} {schema}'
            self.cursor.execute(sql)
        self.connection.commit()

    def _get_db_queue(self):
        self.cursor.execute('SELECT * FROM queue')
        return self.cursor.fetchall()

    def _get_task_func(self, task_name: str) -> Callable:
        for task in self.tasks:
            if task.name == task_name:
                return task.func
        else:
            raise ValueError(f'task {task_name} not in tasklite')

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
        available_workers = self.available_workers
        if available_workers:
            tasks_to_execute = self._get_db_queue()[: len(available_workers)]
            print(tasks_to_execute)
            print(len(tasks_to_execute))
            print(available_workers)
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

                    self.cursor.execute('DELETE FROM queue WHERE id = ?', (task_id,))
                    self.connection.commit()
        else:
            print('Waiting for available workers')

    def flush_db(self):
        self.cursor.execute('DELETE FROM message')
        self.cursor.execute('DELETE FROM queue')
        self.cursor.execute('DELETE FROM results')
        self.connection.commit()
