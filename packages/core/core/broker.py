# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

from abc import ABC, abstractmethod


class Broker(ABC):
    @abstractmethod
    def get_bars(self): ...


def hello() -> str:
    return "Hello from Broker"
