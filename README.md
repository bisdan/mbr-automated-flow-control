# Membrane Bioreactor (MBR) Automated Flow-Rate Control Scripts for Feed, Filtrate, and Bleed

## Overview
This repository contains three asynchronous Python control scripts used to regulate volumetric flow rates for **feed medium**, **filtrate**, and **bleed** streams on the UniteLabs platform.

Each script:
1. Connects to a peristaltic pump channel and a corresponding balance.
2. Continuously records measured bottle weight.
3. Estimates actual flow rate from recent weight-vs-time data using linear regression.
4. Compares estimated flow to a target setpoint and applies bounded corrections when quality criteria are met.
5. Writes controller metrics and flow-rate telemetry to InfluxDB and stores local CSV history.

## Experimental Context
To automate regulation of feed, filtrate, and bleed set flow rates, the scripts are used with the UniteLabs platform (Unite Labs GmbH, Munich, Germany), a peristaltic pump (Reglo ICC 4-channel, Ismatec, Grevenbroich, Germany), and platform scales (DS 20K0-1, Kern & Sohn GmbH, Balingen, Germany).

Weight is sampled every 10-20 seconds (depending on stream), and regression is performed over the most recent 120 measurements. When deviations between target and measured flow are detected, a correction factor is calculated and a new setpoint is applied within configured limits.

## Repository Contents
- `feed_control.py`: feed stream controller
- `filtrate_control.py`: filtrate stream controller
- `bleed_control.py`: bleed stream controller
- `commons.py`: shared logger, scheduler, and InfluxDB write helper
- `feed_control.ipynb`, `filtrate_control.ipynb`, `bleed_control.ipynb`: source notebooks
- `context.txt`: contextual description for manuscript/repository metadata

## Requirements

**Important: The Unitelabs SDK is not publicly available. Contact [UniteLabs](https://unitelabs.io) to obtain access and installation instructions.**

The following versions are required:
- Python `3.11.3`
- unitelabs-lib `0.1.27`
- unitelabs-sdk `0.2.9`
- pandas `2.0.3`
- scikit-learn `1.3.2`
- statsmodels `0.14.0`

Additional runtime dependencies used by the scripts/shared module:
- `influxdb-client[async]`

## Installation
Create and activate a virtual environment, then install dependencies.

Install the Unitelabs SDK (`0.2.9`) using the private distribution method provided by UniteLabs.

Then install the remaining dependencies:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install \
  pandas==2.0.3 \
  scikit-learn==1.3.2 \
  statsmodels==0.14.0 \
  "influxdb-client[async]"
```

Notes:
- `results/` is created automatically by each controller script.
- `logs/` is created automatically by `commons.get_logger(...)`.

## Stream-to-Device Mapping
Current hardcoded service names and channels:

| Stream | Script | Pump Service Name | Pump Channel | Balance Service Name |
|---|---|---|---|---|
| Feed | `feed_control.py` | `Reglo ICC 4x Anaerob` | `pump_actuator_controller_2` | `KERN DS-20k0.1 Feed` |
| Filtrate | `filtrate_control.py` | `Reglo ICC 4x ALE 1` | `pump_actuator_controller_3` | `KERN DS-20k0.1 Bleed`* |
| Bleed | `bleed_control.py` | `Reglo ICC 4x ALE 2` | `pump_actuator_controller_1` | `MT_Viper_SW` |

\* `filtrate_control.py` currently uses the Bleed scale name intentionally (see inline TODO in code).

## Control Logic Summary
Each script follows the same high-level sequence:
1. Connect pump and balance services.
2. Ensure channel addressing mode is enabled.
3. Initialize pump setpoint and start pumping.
4. Start asynchronous subscriptions for flow-rate and weight updates.
5. Run timed loop (`INTERVAL`, `DURATION`) and append local history.
6. Once at least `N_LAST` points are available:
   - fit linear model to recent weight trajectory
   - derive actual flow rate from slope
   - compute correction factor (bounded to `[0.5, 2.0]`)
   - apply setpoint adjustment when fit quality threshold and cooldown criteria are met
7. Continuously write metrics to InfluxDB.
8. On exit/error/interrupt, stop pump and cancel subscriptions.

## Per-Script Parameters
| Parameter | Feed | Filtrate | Bleed |
|---|---:|---:|---:|
| `TARGET_FLOW_RATE` (mL/min) | `0.88` | `0.66` | `0.15` |
| `INTERVAL` (s) | `10` | `10` | `20` |
| `N_LAST` (samples) | `120` | `120` | `120` |
| `MIN_FLOW_RATE_CHANGE_INTERVAL` (s) | `1800` | `1800` | `1800` |
| `R² threshold for correction` | `0.975` | `0.975` | `0.65` |

## Running the Controllers
Run individual controllers from the repository root:

```bash
python bleed_control.py
python feed_control.py
python filtrate_control.py
```

Recommended practice:
1. Run one stream at a time during setup/validation.
2. Confirm services connect correctly before long runs.
3. Stop with `Ctrl+C` and verify pump shutdown in logs.

## Data Outputs
### Local CSV
Each script writes a complete history file to `results/` with columns including:
- `timestamp`
- `time rel.`
- `flow_rate_set`
- `weight_measured`
- `weight_expected`
- `weight_diff_abs`
- `set_flow_rate`
- `actual_flow_rate`
- `r_sq`
- `correction_factor`

### InfluxDB
Telemetry is written via `commons.write_to_database(...)` to the database configured in `commons.py`.

Measurements used:
- `controller`:
  - `r_squared`
  - `correction_factor`
- `flow-rates`:
  - `actual_flow_rate`
  - `set_flow_rate`
  - `flow_rate_setpoint`

## Configuration Before Running
Review and adapt the following before production use:
1. Replace all placeholder InfluxDB values in `commons.py` (`database` dict).
2. Device service names in each script (`connect(name=...)`).
3. Control constants (`TARGET_FLOW_RATE`, limits, interval, duration).

If `commons.py` still contains placeholder InfluxDB values, `write_to_database(...)` raises a `RuntimeError` at runtime.

## Operational Notes
- Correction updates are skipped when estimated actual flow rate is near zero (guard against division-by-zero).
- Stream subscriptions are automatically recreated when timeout tasks complete.
- Scripts are designed for long runs (`DURATION = 600 * 60 * 60` seconds).
- `Ctrl+C` triggers cleanup/shutdown logic with pump stop attempts.

## Publication Checklist
Before public release:
1. Confirm balance mapping for filtrate reflects final hardware setup. (the filtrate script is currently reading weight from a service named Bleed.)

## Citation
**Placeholder:** Final citation (authors, title, journal, year, DOI) will be added here once available.
