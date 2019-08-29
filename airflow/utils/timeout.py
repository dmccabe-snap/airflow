# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import ctypes
import os
import signal
import threading
import time

from airflow.exceptions import AirflowTaskTimeout
from airflow.utils.log.logging_mixin import LoggingMixin


class timeout(LoggingMixin):
    """
    To be used in a ``with`` block and timeout its content.
    """

    def __init__(self, seconds=1, error_message='Timeout'):
        self.seconds = seconds
        self.error_message = error_message + ', PID: ' + str(os.getpid())
        self._finished = False
        self._timeout_end = None
        self._main_thread_id = None
        self._timeout_cv = None
        self._timeout_thread = None

    def handle_timeout(self, signum, frame):
        self.log.error("Process timed out, PID: %s", str(os.getpid()))
        raise AirflowTaskTimeout(self.error_message)

    def __enter__(self):
        if 'alarm' in signal.__dict__:
            try:
                signal.signal(signal.SIGALRM, self.handle_timeout)
                signal.alarm(self.seconds)
            except ValueError as e:
                self.log.warning("timeout can't be used in the current context")
                self.log.exception(e)
        else:
            # Windows does not have SIGALRM, so fake it with a thread and an async exception
            self._finished = False
            self._timeout_end = time.time() + self.seconds
            self._main_thread_id = ctypes.pythonapi.PyThread_get_thread_ident()
            self._timeout_cv = threading.Condition()
            self._timeout_thread = threading.Thread(target=self._wait_for_timeout)
            self._timeout_thread.start()

    def __exit__(self, type, value, traceback):
        if self._timeout_thread:
            with self._timeout_cv:
                self._finished = True
                self._timeout_cv.notify()
            self._timeout_thread.join()
        else:
            try:
                signal.alarm(0)
            except ValueError as e:
                self.log.warning("timeout can't be used in the current context")
                self.log.exception(e)

    def _wait_for_timeout(self):
        with self._timeout_cv:
            while not self._finished:
                time_remaining = self._timeout_end - time.time()
                if time_remaining < 0:
                    self.log.error("Process timed out, PID: %s", str(os.getpid()))
                    result = ctypes.pythonapi.PyThreadState_SetAsyncExc(
                        self._main_thread_id, ctypes.py_object(AirflowTaskTimeout))
                    if not result:
                        self.log.error("Failed to raise exception for thread %d", self._main_thread_id)
                    break
                self._timeout_cv.wait(time_remaining)
