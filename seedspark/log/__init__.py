# -*- coding: utf-8 -*-
"""This module provides unified access to simple to use but powerful logging.
The supplied logger instance is actually an instance of :class:`~loguru._logger.Logger`
from `Loguru <https://loguru.readthedocs.io>`_. This library allows us to perform
advanced structured logging and render informative traceback information to actually
make our logs *worth the effort*.
>>> from seedspark.log import log_instance as log
>>> log.info("Some kind of informational message")
Of course, all the standard logging levels are available...
>>> log.trace
>>> log.debug
>>> log.info
>>> log.warning
>>> log.success
>>> log.error
>>> log.critical
>>> log.exception
Any kind of addtional information that needs to be added to the ``extras`` field of the
resulting log record can be done quickly and easily using the
:meth:`~loguru._logger.Logger.bind` method...
>>> ip_address = user.get_ip()  # hypothetical method
>>> log.bind(ip=ip_address).info(f"User {user.id} has just logged in")
Nifty exception logging can be done for an entire method by using the
:meth:`~loguru._logger.Logger.catch` method (just be sure to stay Pythonic and always
re-raise the caught exception)...
>>> @log.catch(reraise=True)
... def failing_method():
...     return 1 / 0
Other features are also available, please checkout
`the loguru documentation <https://loguru.readthedocs.io>`_.
"""

from seedspark.log.client import configure_logger, get_logger, log_instance

__all__ = ["get_logger", "configure_logger", "log_instance"]
