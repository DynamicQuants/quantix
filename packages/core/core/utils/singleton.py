# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

from functools import wraps
from typing import Any, Callable, Dict, Type, TypeVar

T = TypeVar("T")


def singleton(cls: Type[T]) -> Callable[[], T]:
    """
    Singleton decorator for classes.

    Use this decorator to ensure that only one instance of a class is created. This is known
    pattern in software design.

    For example:

        @singleton
        class MyClass:
            pass
    """

    # Global dictionary to store instances of singleton classes.
    instances: Dict[Type[T], T] = {}

    @wraps(cls)
    def get_instance(*args: Any, **kwargs: Any) -> T:
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return get_instance
