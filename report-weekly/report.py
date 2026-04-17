#!/usr/bin/env python3
"""Weekly report generator.

Reads the 'Финансовая таблица' (SRC) and writes a grouped-by-manager snapshot
to the 'Отчет клод' spreadsheet (DST). Each month lives on its own sheet;
each invocation appends a new snapshot block to that sheet with deltas vs.
the previous snapshot.

Usage:
    report.py fetch --month Март [--year 2026]
        Print JSON snapshot to stdout (+ previous snapshot if present).

    report.py write --month Март [--year 2026] --analysis-file PATH
        Compute snapshot, append a new block to the target sheet.
        The analysis file's contents go under the table as a merged cell.
"""
import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

SRC_SHEET_ID = "14bZiqurDD9_tMJ6OiScWf4-TL2JQ2Wo6MeXyc_ti8KA"
DST_SHEET_ID = "1k8sK-39m0SP1EfV4J_IjKbWm9HMmufoZOx2WYZG5ais"

COL_MONTH, COL_FIO, COL_NUMBER, COL_PARTNER, COL_MANAGER = 0, 1, 2, 3, 4
COL_PLAN, COL_FACT, COL_DEBT = 5, 6, 7

SERVICE_ACCOUNT_PATH = Path.home() / "Desktop" / "receipt-bot" / "service_account.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

REPORT_COLUMNS = [
    "Менеджер", "Принято", "Оплачено", "Долг (шт)", "% сбора",
    "Сумма принято", "Сумма оплачено", "Сумма долг",
]


def parse_money(v):
    if not v:
        return 0.0
    s = str(v).strip().replace("\xa0", "")
    if s.startswith("р."):
        s = s[2:]
    elif s.startswith("р"):
        s = s[1:]
    s = s.replace(" ", "")
    cleaned = re.sub(r"[^\d.,-]", "", s)
    cleaned = cleaned.replace(",", ".")
    try:
        return float(cleaned) if cleaned else 0.0
    except ValueError:
        return 0.0


def fmt_money(x):
    return "р." + f"{int(x):,}".replace(",", "\u00a0")


def _delta_int(cur, prev):
    if prev is None:
        return str(cur)
    d = cur - prev
    return f"{cur} ({d:+d})" if d else f"{cur} (0)"


def _delta_money(cur, prev):
    if prev is None:
        return fmt_money(cur)
    d = cur - prev
    if d == 0:
        return f"{fmt_money(cur)} (0)"
    sign = "+" if d > 0 else "-"
    abs_formatted = f"{int(abs(d)):,}".replace(",", "\u00a0")
    return f"{fmt_money(cur)} ({sign}{abs_formatted})"


def _delta_rate(cur, prev):
    if prev is None:
        return f"{cur:.1f}%"
    d = cur - prev
    if abs(d) < 0.05:
        return f"{cur:.1f}% (0)"
    return f"{cur:.1f}% ({d:+.1f})"


def get_client():
    creds = Credentials.from_service_account_file(str(SERVICE_ACCOUNT_PATH), scopes=SCOPES)
    return gspread.authorize(creds)


def collect_snapshot(month, year):
    gc = get_client()
    ws = gc.open_by_key(SRC_SHEET_ID).worksheet(str(year))
    rows = ws.get_all_values()[1:]

    mgrs = {}
    for r in rows:
        if len(r) <= COL_MANAGER:
            continue
        if r[COL_MONTH].strip() != month:
            continue
        name = r[COL_MANAGER].strip()
        if not name:
            continue
        plan = parse_money(r[COL_PLAN]) if len(r) > COL_PLAN else 0
        fact = parse_money(r[COL_FACT]) if len(r) > COL_FACT else 0
        debt = parse_money(r[COL_DEBT]) if len(r) > COL_DEBT else 0

        m = mgrs.setdefault(name, {
            "name": name,
            "accepted_count": 0, "paid_count": 0,
            "accepted_sum": 0.0, "paid_sum": 0.0, "debt_sum": 0.0,
        })
        m["accepted_count"] += 1
        if fact > 0:
            m["paid_count"] += 1
        m["accepted_sum"] += plan
        m["paid_sum"] += fact
        m["debt_sum"] += debt

    for m in mgrs.values():
        m["debt_count"] = m["accepted_count"] - m["paid_count"]
        m["collection_rate"] = (
            m["paid_count"] / m["accepted_count"] * 100 if m["accepted_count"] else 0.0
        )

    totals = {"accepted_count": 0, "paid_count": 0, "accepted_sum": 0.0, "paid_sum": 0.0, "debt_sum": 0.0}
    for m in mgrs.values():
        for k in totals:
            totals[k] += m[k]
    totals["debt_count"] = totals["accepted_count"] - totals["paid_count"]
    totals["collection_rate"] = (
        totals["paid_count"] / totals["accepted_count"] * 100 if totals["accepted_count"] else 0.0
    )

    ordered = sorted(mgrs.values(), key=lambda m: -m["accepted_count"])
    return {"managers": ordered, "totals": totals}


def _first_int(cell):
    if not cell:
        return 0
    m = re.search(r"-?[\d\s\u00a0]+", str(cell))
    if not m:
        return 0
    try:
        return int(re.sub(r"[\s\u00a0]", "", m.group(0)))
    except ValueError:
        return 0


def _first_float(cell):
    if not cell:
        return 0.0
    m = re.search(r"-?\d+(?:[.,]\d+)?", str(cell))
    return float(m.group(0).replace(",", ".")) if m else 0.0


def parse_previous_snapshot(dst_ws):
    """Scan target sheet for last 'Срез на ...' block. Returns dict or None."""
    values = dst_ws.get_all_values()
    starts = [i for i, row in enumerate(values) if row and row[0].startswith("Срез на ")]
    if not starts:
        return None
    start = starts[-1]
    date_match = re.search(r"Срез на (\d{2}\.\d{2}\.\d{4})", values[start][0])
    prev_date = date_match.group(1) if date_match else None

    managers = {}
    i = start + 2  # skip date row + header row
    while i < len(values):
        row = values[i]
        if not row or not row[0].strip():
            i += 1
            continue
        if row[0].strip() == "ИТОГО":
            break
        if row[0].strip().startswith(("Анализ", "Срез на ")):
            break
        name = row[0].strip()
        pad = row + [""] * (8 - len(row))
        managers[name] = {
            "accepted_count": _first_int(pad[1]),
            "paid_count": _first_int(pad[2]),
            "debt_count": _first_int(pad[3]),
            "collection_rate": _first_float(pad[4]),
            "accepted_sum": _first_int(pad[5]),
            "paid_sum": _first_int(pad[6]),
            "debt_sum": _first_int(pad[7]),
        }
        i += 1
    return {"date": prev_date, "managers": managers}


def _get_or_create_sheet(spreadsheet, title):
    for ws in spreadsheet.worksheets():
        if ws.title == title:
            return ws, False
    ws = spreadsheet.add_worksheet(title=title, rows=200, cols=10)
    return ws, True


def _manager_row(mgr, prev):
    """Build row for a manager with optional deltas vs. prev snapshot."""
    p = prev.get(mgr["name"]) if prev else None
    return [
        mgr["name"],
        _delta_int(mgr["accepted_count"], p["accepted_count"] if p else None),
        _delta_int(mgr["paid_count"], p["paid_count"] if p else None),
        _delta_int(mgr["debt_count"], p["debt_count"] if p else None),
        _delta_rate(mgr["collection_rate"], p["collection_rate"] if p else None),
        _delta_money(mgr["accepted_sum"], p["accepted_sum"] if p else None),
        _delta_money(mgr["paid_sum"], p["paid_sum"] if p else None),
        _delta_money(mgr["debt_sum"], p["debt_sum"] if p else None),
    ]


def _totals_row(totals, prev):
    p = prev["totals"] if prev and "totals" in prev else None
    return [
        "ИТОГО",
        _delta_int(totals["accepted_count"], p["accepted_count"] if p else None),
        _delta_int(totals["paid_count"], p["paid_count"] if p else None),
        _delta_int(totals["debt_count"], p["debt_count"] if p else None),
        _delta_rate(totals["collection_rate"], p["collection_rate"] if p else None),
        _delta_money(totals["accepted_sum"], p["accepted_sum"] if p else None),
        _delta_money(totals["paid_sum"], p["paid_sum"] if p else None),
        _delta_money(totals["debt_sum"], p["debt_sum"] if p else None),
    ]


def cmd_fetch(args):
    snap = collect_snapshot(args.month, args.year)
    gc = get_client()
    dst = gc.open_by_key(DST_SHEET_ID)
    sheet_title = f"{args.month} {args.year}"
    prev = None
    for ws in dst.worksheets():
        if ws.title == sheet_title:
            prev = parse_previous_snapshot(ws)
            break
    out = {
        "month": args.month,
        "year": args.year,
        "today": date.today().strftime("%d.%m.%Y"),
        "current": snap,
        "previous": prev,
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))


def _compute_prev_totals(prev_managers):
    t = {k: 0 for k in ("accepted_count", "paid_count", "debt_count", "accepted_sum", "paid_sum", "debt_sum")}
    for m in prev_managers.values():
        for k in t:
            t[k] += m[k]
    t["collection_rate"] = (
        t["paid_count"] / t["accepted_count"] * 100 if t["accepted_count"] else 0.0
    )
    return t


def cmd_write(args):
    snap = collect_snapshot(args.month, args.year)
    analysis = Path(args.analysis_file).read_text(encoding="utf-8").strip() if args.analysis_file else ""

    gc = get_client()
    dst = gc.open_by_key(DST_SHEET_ID)
    sheet_title = f"{args.month} {args.year}"
    ws, created = _get_or_create_sheet(dst, sheet_title)

    prev = None if created else parse_previous_snapshot(ws)
    prev_for_row = prev["managers"] if prev else None
    prev_totals_wrapped = (
        {"totals": _compute_prev_totals(prev["managers"])} if prev and prev["managers"] else None
    )

    existing = ws.get_all_values()
    # Find first empty row (1-based). If sheet is blank → row 1.
    next_row = 1
    for i, row in enumerate(existing):
        if any(c.strip() for c in row):
            next_row = i + 2  # last non-empty +1
    # But if there are non-empty rows above with gaps, just start at last non-empty + 2 (1 blank separator)
    if any(any(c.strip() for c in r) for r in existing):
        # find last non-empty row
        last_non_empty = max(i for i, r in enumerate(existing) if any(c.strip() for c in r))
        next_row = last_non_empty + 3  # +1 to be 1-based, +2 for blank separator
    else:
        next_row = 1

    today = date.today().strftime("%d.%m.%Y")
    rows_to_write = []
    rows_to_write.append([f"Срез на {today}"] + [""] * 7)  # row 0
    rows_to_write.append(REPORT_COLUMNS)  # row 1 (header)
    for m in snap["managers"]:
        rows_to_write.append(_manager_row(m, prev_for_row))
    rows_to_write.append(_totals_row(snap["totals"], prev_totals_wrapped))
    # Analysis row (will be merged A:H)
    analysis_row_idx = len(rows_to_write)
    rows_to_write.append([f"Анализ: {analysis}"] + [""] * 7)

    # Write block
    start_a1 = f"A{next_row}"
    end_col_letter = "H"
    end_row = next_row + len(rows_to_write) - 1
    ws.update(f"{start_a1}:{end_col_letter}{end_row}", rows_to_write, value_input_option="USER_ENTERED")

    # Merge analysis row A:H
    merge_row = next_row + analysis_row_idx
    ws.merge_cells(f"A{merge_row}:H{merge_row}", merge_type="MERGE_ALL")

    # Text formatting: wrap for analysis, bold for header/totals/date
    requests = [
        {
            "repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": merge_row - 1, "endRowIndex": merge_row,
                          "startColumnIndex": 0, "endColumnIndex": 8},
                "cell": {"userEnteredFormat": {
                    "wrapStrategy": "WRAP",
                    "verticalAlignment": "TOP",
                    "textFormat": {"italic": True},
                }},
                "fields": "userEnteredFormat(wrapStrategy,verticalAlignment,textFormat)",
            }
        },
        # Bold date row
        {
            "repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": next_row - 1, "endRowIndex": next_row,
                          "startColumnIndex": 0, "endColumnIndex": 8},
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True, "fontSize": 12}}},
                "fields": "userEnteredFormat.textFormat",
            }
        },
        # Bold header row
        {
            "repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": next_row, "endRowIndex": next_row + 1,
                          "startColumnIndex": 0, "endColumnIndex": 8},
                "cell": {"userEnteredFormat": {
                    "textFormat": {"bold": True},
                    "backgroundColor": {"red": 1.0, "green": 0.95, "blue": 0.6},
                }},
                "fields": "userEnteredFormat(textFormat,backgroundColor)",
            }
        },
        # Bold ИТОГО row (row index in the block: header + N managers → index = 1 + len(managers))
        {
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": next_row + 1 + len(snap["managers"]),
                    "endRowIndex": next_row + 2 + len(snap["managers"]),
                    "startColumnIndex": 0, "endColumnIndex": 8,
                },
                "cell": {"userEnteredFormat": {
                    "textFormat": {"bold": True},
                    "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9},
                }},
                "fields": "userEnteredFormat(textFormat,backgroundColor)",
            }
        },
    ]
    dst.batch_update({"requests": requests})

    print(json.dumps({
        "sheet": sheet_title,
        "created": created,
        "start_row": next_row,
        "end_row": end_row,
        "managers_count": len(snap["managers"]),
        "previous_date": prev["date"] if prev else None,
    }, ensure_ascii=False, indent=2))


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    f = sub.add_parser("fetch", help="Print snapshot JSON to stdout")
    f.add_argument("--month", required=True)
    f.add_argument("--year", type=int, default=date.today().year)

    w = sub.add_parser("write", help="Append snapshot block to target sheet")
    w.add_argument("--month", required=True)
    w.add_argument("--year", type=int, default=date.today().year)
    w.add_argument("--analysis-file", help="Path to file with analysis text")

    args = p.parse_args()
    if args.cmd == "fetch":
        cmd_fetch(args)
    else:
        cmd_write(args)


if __name__ == "__main__":
    main()
