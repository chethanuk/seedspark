# -*- coding: utf-8 -*-

from onetl.connection import Postgres as OnetlPostgres
from onetl.db import DBReader as OnetlDBReader
from onetl.db import DBWriter as OnetlDBWriter
from onetl.strategy import IncrementalBatchStrategy as OnetlIncrementalBatchStrategy
from onetl.strategy import IncrementalStrategy as OnetlIncrementalStrategy
from onetl.strategy import SnapshotBatchStrategy as OnetlSnapshotBatchStrategy
from onetl.strategy import SnapshotStrategy as OnetlSnapshotStrategy


class DBReader(OnetlDBReader):
    """
    Class for reading data from a database.

    Methods
    -------
    read(query : str) -> Any
        Executes the given query and returns the results.
    """

    def read(self, query: str):
        """Executes the given query and returns the results."""
        return super().read(query)


class DBWriter(OnetlDBWriter):
    """
    Class for writing data to a database.

    Methods
    -------
    write(data : any, table_name : str) -> None
        Writes the given data to the specified table.
    """

    def write(self, data: any, table_name: str):
        """Writes the given data to the specified table."""
        return super().write(data, table_name)


class Postgres(OnetlPostgres):
    """
    Class for interacting with a Postgres database.

    Parameters
    ----------
    host : str, optional
        Database host address (default is 'localhost').
    port : int, optional
        Database port number (default is 5432).
    user : str, optional
        Database user (default is 'user').
    password : str, optional
        Database password (default is 'password').
    database : str, optional
        Database name (default is 'db').
    **kwargs : dict
        Additional keyword arguments.

    Methods
    -------
    connect() -> None
        Establishes a connection to the Postgres database.
    disconnect() -> None
        Closes the connection to the Postgres database.
    """

    def __init__(
        self, host="localhost", port=5432, user="user", password="password", database="db", **kwargs
    ):
        super().__init__(host=host, port=port, user=user, password=password, database=database, **kwargs)


class SnapshotStrategy(OnetlSnapshotStrategy):
    """
    Class for implementing a snapshot batch strategy.

    Methods
    -------
    execute() -> None
        Executes the snapshot batch strategy.
    """

    def execute(self):
        """Executes the snapshot batch strategy."""
        return super().execute()


class SnapshotBatchStrategy(OnetlSnapshotBatchStrategy):
    """
    Class for implementing a snapshot batch strategy.

    Methods
    -------
    execute() -> None
        Executes the snapshot batch strategy.
    """

    def execute(self):
        """Executes the snapshot batch strategy."""
        return super().execute()


class IncrementalStrategy(OnetlIncrementalStrategy):
    """
    Class for implementing an incremental strategy.

    Methods
    -------
    execute() -> None
        Executes the incremental strategy.
    """

    def execute(self):
        """Executes the incremental strategy."""
        return super().execute()


class IncrementalBatchStrategy(OnetlIncrementalBatchStrategy):
    """
    Class for implementing an incremental batch strategy.

    Methods
    -------
    execute() -> None
        Executes the incremental batch strategy.
    """

    def execute(self):
        """Executes the incremental batch strategy."""
        return super().execute()
