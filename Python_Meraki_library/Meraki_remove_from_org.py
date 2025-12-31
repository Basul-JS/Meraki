# Meraki Inventory Cleanup (Unassigned removal + serial utilities)
# Refactor date: 2025-09-02
# Requires: meraki, pyfiglet, rich, openpyxl
"""
Meraki inventory: filter & release unassigned devices (SDK version, Pylance-clean)

- Lists organizations and lets the user choose one
- Optionally filters unassigned devices by "claimed more than N months ago"
- Optional model filter supporting prefixes and wildcards (e.g., MR, MX2, MR4*)
- Exports preview CSV (optional)
- Dry-run preview, then confirmation
- Releases from org inventory in batches, logs results to CSV

Requires: pip install meraki
"""
#!/usr/bin/env python3
from __future__ import annotations

import csv
import re
import sys
import time
from datetime import datetime, timedelta, UTC
from getpass import getpass
from typing import Any, Dict, Iterator, List, Optional, cast
import logging
import meraki

# ---------- Logging ----------
log_filename: str = f"meraki_inventory_script_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def log_and_print(message: str, level: str = 'info') -> None:
    print(message)
    if level == 'debug':
        logging.debug(message)
    elif level == 'info':
        logging.info(message)
    elif level == 'warning':
        logging.warning(message)
    elif level == 'error':
        logging.error(message)
    else:
        logging.info(message)

def safe_exit(code: int = 0, msg: Optional[str] = None) -> None:
    if msg:
        log_and_print(msg, level='info')
    log_and_print("Done. Full log written to: " + log_filename, level='info')
    sys.exit(code)

# ---------- Helpers ----------
def get_str(d: Dict[str, Any], key: str, default: str = "") -> str:
    v = d.get(key)
    return default if v is None else str(v)

def parse_meraki_ts(ts: str) -> datetime:
    if not ts:
        raise ValueError("empty timestamp")
    s = ts.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        m = re.match(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})", s)
        if not m:
            raise
        s2 = m.group(1) + "+00:00"
        dt = datetime.fromisoformat(s2)
    return dt.astimezone(UTC)

def chunked(seq: List[Any], size: int) -> Iterator[List[Any]]:
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def matches_model_prefix(model: str, tokens: List[str]) -> bool:
    if not tokens:
        return True
    mu = model.upper()
    for t in tokens:
        if "*" in t:
            pattern = "^" + re.escape(t).replace(r"\*", ".*") + "$"
            if re.match(pattern, mu, re.IGNORECASE):
                return True
        else:
            if mu.startswith(t):
                return True
    return False

# ---------- SDK Setup ----------
print("Meraki API key will not be echoed.")
API_KEY: str = getpass("Enter your Meraki API key (hidden): ").strip()
if not API_KEY:
    safe_exit(1, "No API key provided.")

dashboard: meraki.DashboardAPI = meraki.DashboardAPI(
    api_key=API_KEY,
    base_url="https://api.meraki.com/api/v1",
    output_log=False,
    print_console=False,
    suppress_logging=True,
    wait_on_rate_limit=True,
    maximum_retries=8,
    retry_4xx_error=True,
)

logging.info("Script started (SDK + Pylance-clean).")

# ---------- Step 1: Orgs ----------
orgs: List[Dict[str, Any]] = []
try:
    orgs = dashboard.organizations.getOrganizations()
except Exception as e:
    safe_exit(1, f"Error fetching organizations: {e}")

if not orgs:
    safe_exit(1, "No organizations returned.")

print("Organizations:")
for idx, org in enumerate(orgs, 1):
    name: str = get_str(org, "name", "<no name>")
    oid: str = get_str(org, "id", "<no id>")
    print(f"{idx}. {name} (ID: {oid})")

MAX_ORG_ATTEMPTS: int = 3
org_attempts: int = 0
orgid_opt: Optional[str] = None

while org_attempts < MAX_ORG_ATTEMPTS:
    try:
        choice_str: str = input("Select organization by number: ").strip()
        org_choice: int = int(choice_str)
        if 1 <= org_choice <= len(orgs):
            orgid_opt = get_str(orgs[org_choice - 1], "id")
            break
        else:
            org_attempts += 1
            if org_attempts == 1:
                log_and_print("Warning: invalid selection. Choose a number from the list.", 'warning')
            elif org_attempts >= MAX_ORG_ATTEMPTS:
                safe_exit(1, "No valid organization selected after 3 attempts. Exiting.")
            else:
                print(f"Invalid selection. {MAX_ORG_ATTEMPTS - org_attempts} attempt(s) left.")
    except ValueError:
        org_attempts += 1
        if org_attempts == 1:
            log_and_print("Warning: input must be a number corresponding to the list.", 'warning')
        elif org_attempts >= MAX_ORG_ATTEMPTS:
            safe_exit(1, "No valid organization selected after 3 attempts. Exiting.")
        else:
            print(f"Invalid input. {MAX_ORG_ATTEMPTS - org_attempts} attempt(s) left.")
    except KeyboardInterrupt:
        safe_exit(1, "Cancelled by user.")

if orgid_opt is None or orgid_opt == "":
    safe_exit(1, "No valid organization selected. Exiting.")

assert orgid_opt is not None
orgid_str: str = orgid_opt

# ---------- Step 2: Unassigned devices ----------
print("Retrieving unassigned (unused) inventory devices...")
devices: List[Dict[str, Any]] = []
from typing import cast
try:
    devices = dashboard.organizations.getOrganizationInventoryDevices(
        orgid_str,
        usedState='unused',
        perPage=1000,
        total_pages=cast(int, "all")
    )
except TypeError:
    devices_all: List[Dict[str, Any]] = dashboard.organizations.getOrganizationInventoryDevices(
        orgid_str, perPage=1000, total_pages=cast(int, "all")
    )
    devices = [d for d in devices_all if get_str(d, "networkId", "") == ""]
except Exception as e:
    safe_exit(1, f"Error fetching inventory devices: {e}")

log_and_print(f"Found {len(devices)} unassigned devices in inventory.")

# ---------- Step 3: Age filter ----------
user_input: str = input("Show only devices claimed more than how many months ago? (press Enter to skip): ").strip()
months: Optional[int] = None
if user_input:
    try:
        months = int(user_input)
    except ValueError:
        print("Invalid number. Showing all unassigned devices.")
        months = None

unassigned_devices: List[Dict[str, Any]] = []
cutoff_date: Optional[datetime] = None
if months:
    cutoff_date = datetime.now(UTC) - timedelta(days=months * 30)

for device in devices:
    if months and cutoff_date is not None:
        claimed_at_raw: str = get_str(device, "claimedAt", "")
        if not claimed_at_raw:
            logging.warning(f"No claimedAt for device {get_str(device, 'serial', 'N/A')}")
            continue
        try:
            claimed_dt: datetime = parse_meraki_ts(claimed_at_raw)
        except Exception:
            logging.warning(f"Date parsing failed for device {get_str(device, 'serial', 'N/A')}: {claimed_at_raw}")
            continue
        if claimed_dt < cutoff_date:
            unassigned_devices.append(device)
    else:
        unassigned_devices.append(device)

suffix: str = f" claimed more than {months} months ago." if months else "."
log_and_print(f"Found {len(unassigned_devices)} unassigned devices{suffix}")

if not unassigned_devices:
    safe_exit(0, "Note: there are no devices that fit the criteria (unassigned/age filter). Exiting without changes.")

# ---------- Step 3b: Model filter ----------
all_models: List[str] = sorted({get_str(dev, "model", "N/A") for dev in unassigned_devices})
print(f"\nAvailable models in filtered inventory: {', '.join(all_models) if all_models else '(none)'}")
model_input: str = input(
    "Enter model(s) or prefixes (comma separated; e.g., MG, MR, MX2; wildcard * supported like MR4*). "
    "Leave blank for all: "
).strip().upper()

selected_tokens: List[str] = [m.strip() for m in model_input.split(",") if m.strip()] if model_input else []

filtered_devices: List[Dict[str, Any]] = []
for device in unassigned_devices:
    dev_model: str = get_str(device, "model", "")
    if matches_model_prefix(dev_model, selected_tokens):
        filtered_devices.append(device)

if selected_tokens:
    log_and_print(f"User selected model filters: {', '.join(sorted(selected_tokens))}")
log_and_print(f"Devices eligible for removal after model filter: {len(filtered_devices)}")

if not filtered_devices:
    safe_exit(0, "Note: there are no devices that fit the criteria for removal after model filtering. Exiting without changes.")

# ---------- Step 4: CSV preview ----------
export: str = input("Export these devices to CSV? (y/n): ").strip().lower()
if export == 'y':
    filename: str = f"meraki_unassigned_inventory_{orgid_str}.csv"
    keys: List[str] = sorted(set().union(*(d.keys() for d in filtered_devices))) if filtered_devices else []
    if keys:
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(keys))
            writer.writeheader()
            writer.writerows(filtered_devices)
        log_and_print(f"Filtered inventory exported to {filename}")
    else:
        log_and_print("No devices to export.", level='warning')

# ---------- Step 5: Dry run ----------
print("\n--- DRY RUN: Devices that WOULD be removed ---")
for device in filtered_devices:
    print(
        f"Model: {get_str(device,'model','N/A')}, "
        f"Serial: {get_str(device,'serial','N/A')}, "
        f"MAC: {get_str(device,'mac','N/A')}, "
        f"Name: {get_str(device,'name','N/A')}, "
        f"Claimed At: {get_str(device,'claimedAt','N/A')}"
    )
log_and_print(f"Dry-run previewed {len(filtered_devices)} devices for removal.")

confirm: str = input("\nProceed with ACTUAL removal from the org? (y/n): ").strip().lower()
if confirm != 'y':
    safe_exit(0, "Cancelled by user. Exiting without changes.")

# ---------- Step 6: Release ----------
removal_log: List[Dict[str, str]] = []

def release_serials(organization_id: str, serials: List[str]) -> Dict[str, Any]:
    return cast(Dict[str, Any], dashboard.organizations.releaseFromOrganizationInventory(
        organization_id, serials=serials
    ))

print("\nRemoving devices...")
serials_all: List[str] = [s for s in (get_str(d, "serial", "") for d in filtered_devices) if s]
index: Dict[str, Dict[str, Any]] = {get_str(d, "serial", ""): d for d in filtered_devices if get_str(d, "serial", "")}

for batch in chunked(serials_all, 100):
    try:
        response: Dict[str, Any] = release_serials(orgid_str, batch)
        released: List[str] = response.get('serials', batch) if isinstance(response, dict) else batch
        removed_at: str = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
        for serial in released:
            dev: Dict[str, Any] = index.get(serial, {})
            mac: str = get_str(dev, "mac", "N/A")
            dev_model: str = get_str(dev, "model", "N/A")
            dev_name: str = get_str(dev, "name", "N/A")
            msg: str = f"Removed {serial} (Model: {dev_model}, Name: {dev_name}, MAC: {mac}) at {removed_at}"
            log_and_print(msg)
            removal_log.append({
                "serial": serial,
                "mac": mac,
                "model": dev_model,
                "name": dev_name,
                "removed_at": removed_at
            })
    except Exception as e:
        log_and_print(f"Failed to remove batch of {len(batch)}: {e}", level='error')
    time.sleep(0.2)

if removal_log:
    removal_filename: str = f"meraki_removed_inventory_{orgid_str}.csv"
    with open(removal_filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["serial", "mac", "model", "name", "removed_at"])
        writer.writeheader()
        writer.writerows(removal_log)
    log_and_print(f"\nRemoval log exported to {removal_filename}")
else:
    log_and_print("No devices were removed, so no removal log was created.")

safe_exit(0)
