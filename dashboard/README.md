# Electricity generation dashboard

Visualization of **US electricity generation by fuel/source over time**, using static JSON built from an annual CSV. The chart and copy are driven by checked-in data files, not a live API in the browser.

## Data pipeline

- **Script:** [`scripts/build_eia_aggregate.py`](scripts/build_eia_aggregate.py) reads [`data/eia_annual_input.csv`](data/eia_annual_input.csv) (columns: year, coal, gas, nuclear, hydro, wind, solar, other in GWh) and writes aggregated JSON for the front end.
- **Optional fetch:** With `EIA_API_KEY` set, `--fetch` can download from the **EIA Open Data API v2** and populate the CSV (see script docstring for routes and column mapping).

```bash
# Rebuild JSON from existing CSV
python3 scripts/build_eia_aggregate.py

# Or fetch (requires API key, never commit it)
EIA_API_KEY=your_key python3 scripts/build_eia_aggregate.py --fetch
```

## Artifacts

| File | Role |
|------|------|
| [`data/eia-generation-annual.json`](data/eia-generation-annual.json) | Series consumed by the page |
| [`data/eia-meta.json`](data/eia-meta.json) | Metadata for labels and display |
| [`data/eia_annual_input.csv`](data/eia_annual_input.csv) | Input table for the builder |
| [`index.html`](index.html) | Single-page UI (styles and chart logic inline) |

## Stack

Vanilla HTML/JS for the demo; Python for reproducible data prep only.
