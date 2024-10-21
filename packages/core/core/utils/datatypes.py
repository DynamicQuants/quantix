# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Datatypes module."""

import numpy as np

float32 = np.float32
"""The float32 can store up to 7 decimal digits. This datatype is used to represent values when the
precision is not that important. If you need more precision, use 64-bit floating point numbers like
native Python `float` or `np.float64`."""
