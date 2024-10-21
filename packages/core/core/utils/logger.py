# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

import logging

from colorama import Fore, Style, init

from .singleton import singleton

# Reset the colorama style.
init(autoreset=True)


class _LoggerConfig(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_colors = {
            logging.DEBUG: Fore.BLUE,
            logging.INFO: Fore.GREEN,
            logging.WARNING: Fore.YELLOW,
            logging.ERROR: Fore.RED,
            logging.CRITICAL: Fore.RED + Style.BRIGHT,
        }
        level_color = log_colors.get(record.levelno, "")
        message = super().format(record)
        return f"{level_color}{message}{Style.RESET_ALL}"


@singleton
class _Logger:
    """Provides a simple colored console logger."""

    def __init__(self) -> None:
        self.logger = logging.getLogger("core_logger")
        handler = logging.StreamHandler()
        formatter = _LoggerConfig("%(levelname)s: %(message)s")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)

    def debug(self, message: str) -> None:
        self.logger.debug(message)

    def info(self, message: str) -> None:
        self.logger.info(message)

    def warning(self, message: str) -> None:
        self.logger.warning(message)

    def error(self, message: str) -> None:
        self.logger.error(message)

    def critical(self, message: str) -> None:
        self.logger.critical(message)


Logger = _Logger()
"""Singleton console colored logger."""
