"""Checkpoint only the active scheduler-owned portfolio cycle, never user input."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import sqlite3


class PortfolioCheckpoint:
    def __init__(self, environment=None):
        env = os.environ if environment is None else environment
        self.path = Path(env['WALNUT_JOB_QUEUE']).resolve()
        self.key = env['WALNUT_JOB_KEY']
        self.token = env['WALNUT_JOB_TOKEN']
        self.cycle = env['WALNUT_JOB_CYCLE']
        if not all(re.fullmatch(r'[0-9a-f]{32}', value) for value in (self.token, self.cycle)):
            raise ValueError('Invalid portfolio scheduler identity')

    def _connect(self):
        db = sqlite3.connect(self.path.as_uri() + '?mode=rw', uri=True, timeout=10)
        db.row_factory = sqlite3.Row
        return db

    def read(self):
        db = self._connect()
        try:
            row = db.execute('SELECT checkpoint_json FROM jobs WHERE key=? AND token=? AND cycle_id=? '
                             'AND started_at IS NOT NULL', (self.key, self.token, self.cycle)).fetchone()
            if row is None:
                raise RuntimeError('Portfolio scheduler ownership lost')
            if row['checkpoint_json'] is None:
                return None
            state = json.loads(row['checkpoint_json'])
            if state.get('kind') != 'portfolio_refresh_v1' or state.get('cycle_id') != self.cycle:
                raise RuntimeError('Portfolio checkpoint identity mismatch')
            return state
        finally:
            db.close()

    def save(self, state):
        if state.get('kind') != 'portfolio_refresh_v1' or state.get('cycle_id') != self.cycle:
            raise ValueError('Portfolio checkpoint identity mismatch')
        encoded = json.dumps(state, sort_keys=True, allow_nan=False)
        if len(encoded.encode()) > 1_000_000:
            raise ValueError('Portfolio checkpoint exceeds its size guard')
        db = self._connect()
        try:
            updated = db.execute('UPDATE jobs SET checkpoint_json=? WHERE key=? AND token=? AND cycle_id=? '
                                 'AND started_at IS NOT NULL', (encoded, self.key, self.token, self.cycle))
            if updated.rowcount != 1:
                raise RuntimeError('Portfolio scheduler ownership lost')
            db.commit()
        finally:
            db.close()
