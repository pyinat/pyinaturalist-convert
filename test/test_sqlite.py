import sqlite3

from pyinaturalist_convert.sqlite import vacuum_analyze


def _make_db(db_path):
    with sqlite3.connect(db_path) as conn:
        conn.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)')
        conn.execute("INSERT INTO t VALUES (1, 'x')")


def test_vacuum_analyze__fast_does_not_force_temp_store_memory(tmp_path, monkeypatch):
    """Regression test for #230.

    `vacuum_analyze(fast=True)` used to also set `PRAGMA temp_store = MEMORY`. SQLite's VACUUM
    works by building a full temporary copy of the database before swapping it in; with
    temp_store=MEMORY, that temporary copy is held in RAM instead of on disk, so peak memory
    usage scales with database size. For the real iNaturalist database (tens of GB as of 2026,
    per this module's own docstring), that reliably OOMs even well-provisioned machines.

    `fast=True` should still apply its other, size-independent optimizations (larger page cache,
    multiple sorter threads), just not `temp_store = MEMORY`.
    """
    db_path = tmp_path / 'test.db'
    _make_db(db_path)

    executed = []
    real_connect = sqlite3.connect

    class RecordingConnection(sqlite3.Connection):
        def execute(self, sql, *args, **kwargs):
            executed.append(sql)
            return super().execute(sql, *args, **kwargs)

    def fake_connect(path, *args, **kwargs):
        return real_connect(path, *args, factory=RecordingConnection, **kwargs)

    monkeypatch.setattr(sqlite3, 'connect', fake_connect)

    vacuum_analyze(['t'], db_path, fast=True)

    executed_lower = [stmt.lower() for stmt in executed]
    assert not any('temp_store' in stmt for stmt in executed_lower), (
        f'fast=True must not set PRAGMA temp_store = MEMORY (executed: {executed})'
    )
    # Sanity check: fast=True's other, bounded optimizations should still be applied
    assert any('cache_size' in stmt for stmt in executed_lower)
    assert any(stmt == 'vacuum' for stmt in executed_lower)


def test_vacuum_analyze__runs_successfully(tmp_path):
    """Basic sanity check that vacuum_analyze still works end-to-end for both fast=True/False"""
    for fast in (True, False):
        db_path = tmp_path / f'test_{fast}.db'
        _make_db(db_path)
        vacuum_analyze(['t'], db_path, fast=fast)
        with sqlite3.connect(db_path) as conn:
            row = conn.execute('SELECT data FROM t WHERE id = 1').fetchone()
        assert row == ('x',)
