"""测试公共夹具：每个用例拿到一份干净的示例数据，避免用例之间互相污染。"""
from __future__ import annotations

import copy

import pytest

from app.seed import SEED_ROWS
from app.store import store


@pytest.fixture(autouse=True)
def reset_store():
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
    store._channels.clear()
    store._failures.clear()
    store._submissions.clear()
    yield
    store._tables = {name: copy.deepcopy(rows) for name, rows in SEED_ROWS.items()}
    store._channels.clear()
    store._failures.clear()
    store._submissions.clear()
