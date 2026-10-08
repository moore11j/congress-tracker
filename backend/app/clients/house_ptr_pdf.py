"""Column-aware extraction for the Clerk's digital PTR table.

Coordinates are transformed into page space before grouping. Rows can continue
on the next page; repeated headings and account notes are not transaction cells.
This deliberately does not interpret scanned checkbox forms.
"""
from __future__ import annotations

import re
from bisect import bisect_right


ACTION = re.compile(r"P|E|S(?:\s*\((?:F|P|full|partial)\))?", re.I)
DATE = re.compile(r"\d{2}/\d{2}/\d{4}")
AMOUNT = re.compile(r"(?:Over\s+)?\$[\d,]+(?:\s*-\s*\$[\d,]+)?", re.I)


def extract_house_columns(pages: list[list[tuple[float, float, str]]]) -> list[dict] | None:
    """Return complete raw cells, or None when no recognized table is present.

    A recognized but incomplete table raises ValueError instead of falling back
    to a parser that could accept only a subset of its rows.
    """
    rows: list[dict] = []
    current = None
    recognized = False
    for tokens in pages:
        lines: list[list[tuple[float, float, str]]] = []
        for token in sorted(tokens, key=lambda item: (-item[1], item[0])):
            if not lines or abs(token[1] - lines[-1][0][1]) > 1:
                lines.append([])
            lines[-1].append(token)
        headers = []
        for line in lines:
            ordered = sorted(line)
            labels = [item[2] for item in ordered]
            if labels == ['ID', 'Owner', 'Asset', 'Transaction', 'Date', 'Notification', 'Amount', 'Cap.']:
                headers.append(ordered)
        if not headers:
            if any(re.search(r'\[[A-Z0-9]{1,3}\]', token[2]) for token in tokens):
                if recognized:
                    raise ValueError('House transaction page lacks recognized column headings')
                return None
            continue
        if len(headers) != 1:
            raise ValueError('House column headings are ambiguous')
        recognized = True
        header = headers[0]
        starts = [item[0] for item in header]
        # Data cells sit a fraction of a point left of the heading origins.
        boundaries = [x - 2 for x in starts]
        header_y = header[0][1]
        for line in lines:
            if line[0][1] >= header_y - 24:
                continue  # two further heading lines, ending in $200?
            cells = [[] for _ in starts]
            for x, _, value in sorted(line):
                column = bisect_right(boundaries, x) - 1
                if column >= 0:
                    cells[column].append(value)
            joined = [' '.join(cell) for cell in cells]
            if ACTION.fullmatch(joined[3]):
                if current is not None:
                    rows.append(current)
                current = {"cells": [[] for _ in starts], "sealed": False, "notes": []}
            elif joined[3] and DATE.fullmatch(joined[4]):
                raise ValueError('House transaction action is unrecognized')
            if current is None:
                continue
            note = ' '.join(joined).strip()
            if re.match(r'(?:Filing Status|F S)\s*:', note, re.I):
                current['sealed'] = True
            if current['sealed']:
                if note and not note.startswith('Filing ID #'):
                    current['notes'].append(note)
                continue
            # Footer text sits outside the data cells and must not join a row
            # which continues on the next page.
            if note.startswith('Filing ID #'):
                continue
            for index, parts in enumerate(cells):
                current['cells'][index].extend(parts)
    if not recognized:
        return None
    if current is not None:
        rows.append(current)
    result = []
    for row in rows:
        cells = [' '.join(parts).strip() for parts in row['cells']]
        printed_id, owner, asset, action, transacted, notified, amount, _ = cells
        codes = re.findall(r'\[([A-Z0-9]{1,3})\]', asset)
        if (not row['sealed'] or len(codes) != 1 or not asset.endswith(f'[{codes[0]}]')
                or not ACTION.fullmatch(action) or not DATE.fullmatch(transacted)
                or not DATE.fullmatch(notified) or not AMOUNT.fullmatch(amount)
                or owner not in {'', 'SP', 'DC', 'JT'}
                or (printed_id and not re.fullmatch(r'[1-9][0-9]*', printed_id))):
            raise ValueError('House row cells incomplete or unrecognized')
        result.append(dict(printed_id=printed_id, owner=owner or 'self',
                           asset=asset[:asset.rfind('[')].strip(), asset_type=codes[0],
                           type=action, date=transacted, notification=notified,
                           amount=amount, source_notes='\n'.join(row['notes'])))
    ids = [row['printed_id'] for row in result if row['printed_id']]
    if ids and (len(ids) != len(result) or len(set(ids)) != len(ids)):
        raise ValueError('House printed row identity incomplete or repeated')
    return result
