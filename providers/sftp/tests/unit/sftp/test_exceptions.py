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
from __future__ import annotations

from contextlib import contextmanager
from typing import Any
from unittest import mock

import pytest

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.sftp.exceptions import ConnectionNotOpenedException
from airflow.providers.sftp.hooks.sftp import handle_connection_management


class StubHook:
    """
    Minimal stand-in for ``SFTPHook`` covering only what the decorator touches.

    ``handle_connection_management`` reads ``use_managed_conn``, reads and writes
    ``conn``, and calls ``get_managed_conn``. Stubbing those three keeps the tests
    free of a live SFTP server and of a ``Connection`` record, so they exercise the
    decorator's branching rather than paramiko.
    """

    def __init__(
        self,
        *,
        use_managed_conn: bool,
        conn: Any = None,
        managed_conn: Any = None,
    ) -> None:
        self.use_managed_conn = use_managed_conn
        self.conn = conn
        self.managed_conn = managed_conn
        self.managed_conn_opened = False
        self.managed_conn_closed = False
        self.calls: list[tuple[tuple, dict]] = []
        self.conn_during_call: Any = None

    @contextmanager
    def get_managed_conn(self):
        self.managed_conn_opened = True
        try:
            yield self.managed_conn
        finally:
            self.managed_conn_closed = True

    @handle_connection_management
    def do_work(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        self.conn_during_call = self.conn
        return "result"

    @handle_connection_management
    def explode(self):
        self.conn_during_call = self.conn
        raise RuntimeError("boom")


def test_derives_from_airflow_exception():
    """
    Callers catch ``AirflowException``, so narrowing this base class would be a
    breaking change for them. Pin it.
    """
    assert issubclass(ConnectionNotOpenedException, AirflowException)


def test_raises_when_unmanaged_and_connection_not_opened():
    hook = StubHook(use_managed_conn=False, conn=None)

    with pytest.raises(ConnectionNotOpenedException, match=r"hook\.get_managed_conn\(\)"):
        hook.do_work()

    assert hook.calls == [], "the wrapped function must not run without a connection"
    assert hook.managed_conn_opened is False


def test_delegates_when_unmanaged_and_connection_already_opened():
    conn = mock.MagicMock()
    hook = StubHook(use_managed_conn=False, conn=conn)

    result = hook.do_work("arg", key="value")

    assert result == "result"
    assert hook.calls == [(("arg",), {"key": "value"})]
    assert hook.conn_during_call is conn
    assert hook.managed_conn_opened is False, "an already-open connection must be reused as is"


def test_opens_managed_connection_when_managed():
    managed_conn = mock.MagicMock()
    hook = StubHook(use_managed_conn=True, conn=None, managed_conn=managed_conn)

    result = hook.do_work()

    assert result == "result"
    assert hook.managed_conn_opened is True
    assert hook.managed_conn_closed is True
    assert hook.conn_during_call is managed_conn, (
        "self.conn must be the managed connection while the wrapped function runs"
    )


def test_managed_connection_closed_when_wrapped_function_raises():
    managed_conn = mock.MagicMock()
    hook = StubHook(use_managed_conn=True, managed_conn=managed_conn)

    with pytest.raises(RuntimeError, match="boom"):
        hook.explode()

    assert hook.managed_conn_opened is True
    assert hook.managed_conn_closed is True, "the with block must exit even when the body raises"
