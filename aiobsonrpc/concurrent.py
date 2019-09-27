# -*- coding: utf-8 -*-
"""
This module provides a collection of concurrency related
object generators. These generators will create either
native threading based or greenlet based objects depending
on which threading_model is selected.
"""

import asyncio

__license__ = 'http://mozilla.org/MPL/2.0/'


def new_promise(loop=None):
    loop = asyncio.get_event_loop() if loop is None else loop
    return asyncio.Future(loop=loop)
