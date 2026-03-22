#!/usr/bin/env python3
"""
Rebuild dashboard JSON from data/eia_annual_input.csv, or fetch that CSV from the EIA API.

CSV columns: year, coal, gas, nuclear, hydro, wind, solar, other (GWh; EIA "thousand megawatthours"
uses the same numeric value as GWh).

Fetch mode (--fetch) uses EIA Open Data API v2:
  electricity/electric-power-operational-data
  location=US, sectorid=99 (all sectors), frequency=annual, data=generation

Mapping (residual "other" reconciles to ALL so totals match EIA):
  coal=COW, gas=NGO, nuclear=NUC, hydro=HYC+HPS, wind=WND,
  solar=TSN if present and >0, else TPV if >0, else SUN+SPV+DPV+STH,
  other=round(ALL) − sum of rounded components (non-negative).

Usage:
  python3 scripts/build_eia_aggregate.py
  EIA_API_KEY=your_key python3 scripts/build_eia_aggregate.py --fetch
  EIA_API_KEY=your_key python3 scripts/build_eia_aggregate.py --fetch --end-year 2023
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
CSV_PATH = DATA / "eia_annual_input.csv"
OUT_DATA = DATA / "eia-generation-annual.json"
OUT_META = DATA / "eia-meta.json"

FUELS = ("coal", "gas", "nuclear", "hydro", "wind", "solar", "other")

EIA_V2_DATA_URL = "https://api.eia.gov/v2/electricity/electric-power-operational-data/data/"


def _f(x: Any) -> float:
    if x is None:
        return 0.0
    return float(x)


def solar_gwh(ids: dict[str, float]) -> float:
    tsn = ids.get("TSN")
    if tsn and tsn > 0:
        return tsn
    tpv = ids.get("TPV")
    if tpv and tpv > 0:
        return tpv
    s = 0.0
    for k in ("SUN", "SPV", "DPV", "STH"):
        s += ids.get(k) or 0.0
    return s


def fetch_eia_annual_rows(api_key: str, start_year: int, end_year: int) -> list[dict[str, int]]:
    """Download and collapse API rows into one record per year."""
    params: list[tuple[str, str]] = [
        ("api_key", api_key),
        ("frequency", "annual"),
        ("data[0]", "generation"),
        ("facets[location][]", "US"),
        ("facets[sectorid][]", "99"),
        ("start", str(start_year)),
        ("end", str(end_year)),
        ("length", "5000"),
    ]
    url = EIA_V2_DATA_URL + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            payload = json.load(resp)
    except urllib.error.HTTPError as e:
        raise SystemExit(f"EIA API HTTP {e.code}: {e.read().decode(errors='replace')[:500]}") from e
    except urllib.error.URLError as e:
        raise SystemExit(f"EIA API network error: {e}") from e

    err = payload.get("error")
    if err:
        raise SystemExit(f"EIA API error: {err}")

    rows = payload.get("response", {}).get("data") or []
    by_year: dict[str, dict[str, float]] = defaultdict(dict)
    for r in rows:
        period = r.get("period")
        fid = r.get("fueltypeid")
        if not period or not fid:
            continue
        by_year[period][str(fid)] = _f(r.get("generation"))

    out: list[dict[str, int]] = []
    for y in sorted(by_year.keys(), key=int):
        ids = by_year[y]
        all_g = ids.get("ALL")
        if all_g is None or all_g <= 0:
            continue
        coal = ids.get("COW") or 0.0
        gas = ids.get("NGO") or 0.0
        nuclear = ids.get("NUC") or 0.0
        hydro = (ids.get("HYC") or 0.0) + (ids.get("HPS") or 0.0)
        wind = ids.get("WND") or 0.0
        solar = solar_gwh(ids)

        all_r = int(round(all_g))
        coal_i = int(round(coal))
        gas_i = int(round(gas))
        nuc_i = int(round(nuclear))
        hyd_i = int(round(hydro))
        wind_i = int(round(wind))
        sol_i = int(round(solar))
        other_i = all_r - coal_i - gas_i - nuc_i - hyd_i - wind_i - sol_i
        rec = {
            "year": int(y),
            "coal": coal_i,
            "gas": gas_i,
            "nuclear": nuc_i,
            "hydro": hyd_i,
            "wind": wind_i,
            "solar": sol_i,
            "other": max(0, other_i),
        }
        diff = all_r - sum(rec[k] for k in FUELS)
        if diff:
            rec["other"] = max(0, rec["other"] + diff)
        out.append(rec)

    return out


def write_csv(rows: list[dict[str, int]]) -> None:
    with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["year", *FUELS])
        w.writeheader()
        for r in rows:
            w.writerow({k: r[k] for k in ("year", *FUELS)})
    print(f"Wrote {CSV_PATH.relative_to(ROOT)} ({CSV_PATH.stat().st_size} bytes)")


def read_rows() -> list[dict]:
    rows: list[dict] = []
    with CSV_PATH.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            y = int(row["year"])
            rec = {"year": y}
            for k in FUELS:
                rec[k] = int(row[k])
            rows.append(rec)
    rows.sort(key=lambda r: r["year"])
    return rows


def write_generation_json(rows: list[dict]) -> None:
    payload = {
        "year_min": rows[0]["year"],
        "year_max": rows[-1]["year"],
        "unit": "GWh",
        "fuels": list(FUELS),
        "rows": [{k: r[k] for k in ("year", *FUELS)} for r in rows],
    }
    OUT_DATA.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {OUT_DATA.relative_to(ROOT)} ({OUT_DATA.stat().st_size} bytes)")


def patch_meta(rows: list[dict], *, from_api: bool) -> None:
    if not OUT_META.exists():
        return
    meta = json.loads(OUT_META.read_text(encoding="utf-8"))
    meta["last_data_build"] = date.today().isoformat()
    if from_api:
        meta["methodology_note"] = (
            "Annual net generation (GWh) from EIA Open Data API v2, route "
            "`electricity/electric-power-operational-data`, location=US, sectorid=99 (all sectors), "
            "frequency=annual, field `generation` (thousand megawatthours; numeric = GWh). "
            "Categories: coal=COW (all coal products), gas=NGO (natural gas & other gases), "
            "nuclear=NUC, hydro=HYC+HPS (conventional hydro + pumped storage), wind=WND, "
            "solar=TSN when reported else TPV else SUN+SPV+DPV+STH, "
            "other=residual so the seven fuels sum to EIA total ALL for each year. "
        )
        meta["disclaimer"] = (
            "Pre-aggregated for this static site from the EIA API; not a live query in the browser. "
            "EIA may revise historical values."
        )
        meta["source_urls"] = list(
            dict.fromkeys(
                [
                    "https://www.eia.gov/opendata/browser/electricity/electric-power-operational-data",
                    "https://www.eia.gov/electricity/data.php",
                ]
                + [u for u in meta.get("source_urls", []) if isinstance(u, str)]
            )
        )
    meta["year_range_fetched"] = f"{rows[0]['year']}–{rows[-1]['year']}"
    OUT_META.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
    print(f"Updated {OUT_META.relative_to(ROOT)}")


def validate_rows(rows: list[dict]) -> None:
    for r in rows:
        s = sum(r[k] for k in FUELS)
        if s <= 0:
            raise SystemExit(f"Invalid row year={r['year']}: non-positive sum")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build dashboard electricity JSON (and optional EIA fetch).")
    parser.add_argument(
        "--fetch",
        action="store_true",
        help="Download annual data from the EIA API (requires EIA_API_KEY env var).",
    )
    parser.add_argument("--start-year", type=int, default=2001)
    parser.add_argument(
        "--end-year",
        type=int,
        default=2024,
        help="Last full calendar year to include (default 2024; 2025+ may be incomplete).",
    )
    args = parser.parse_args()

    if args.fetch:
        key = os.environ.get("EIA_API_KEY", "").strip()
        if not key:
            print("Set EIA_API_KEY in the environment (never commit it).", file=sys.stderr)
            raise SystemExit(1)
        if args.start_year > args.end_year:
            raise SystemExit("start-year must be <= end-year")
        rows = fetch_eia_annual_rows(key, args.start_year, args.end_year)
        if not rows:
            raise SystemExit("No rows returned from EIA API for the given range.")
        validate_rows(rows)
        write_csv(rows)
        write_generation_json(rows)
        patch_meta(rows, from_api=True)
        return

    if not CSV_PATH.exists():
        raise SystemExit(f"Missing {CSV_PATH}. Run with --fetch and EIA_API_KEY, or add the CSV.")
    rows = read_rows()
    validate_rows(rows)
    write_generation_json(rows)
    patch_meta(rows, from_api=False)


if __name__ == "__main__":
    main()
