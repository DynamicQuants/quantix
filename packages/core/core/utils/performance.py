# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Performance timer module."""

import time
from functools import wraps
from typing import Any, Callable, Optional

from typing_extensions import Self

from .datatypes import float32
from .logger import Logger


class PerformanceTimer:
    """
    Performance timer class.

    It provides a context manager to measure the execution time of a code block and a decorator
    to measure the execution time of a function.

    Examples:
        >>> with PerformanceTimer("Test"):
        ...     pass
        ...
        >>> with PerformanceTimer():
        ...     pass

        >>> @PerformanceTimer("Decorated function")
        ... def sample_function():
        ...     return "result"
    """

    def __init__(self, name: Optional[str] = None) -> None:
        self.name = name
        self.start_time: float = 0
        self.end_time: float = 0

    def __enter__(self) -> Self:
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.end_time = time.perf_counter()
        elapsed_time = self.end_time - self.start_time
        if self.name:
            Logger.debug(f"{self.name} executed in {elapsed_time:.6f} seconds")
        else:
            Logger.debug(f"Code block executed in {elapsed_time:.6f} seconds")

    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> tuple[Any, float]:
            with self:
                result = func(*args, **kwargs)
            return result, self.end_time - self.start_time

        return wrapper

    @property
    def elapsed_time(self) -> float32:
        return float32(self.end_time - self.start_time)
