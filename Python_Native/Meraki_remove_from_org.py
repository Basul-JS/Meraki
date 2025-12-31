# Created by J A Said
# Date: 2025-08-20
# Description: This script removes unassigned devices from a Meraki organization.

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from getpass import getpass
import csv
import re
import time
from datetime import datetime, timedelta, UTC
import logging
import traceback
import sys
from typing import Any, Dict, List, Iterator, cast

# ---------- Logging Setup ----------
log_filename = f"meraki_inventory_script_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.log"
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
    elif level == 'error':
        logging.error(message)
    elif level == 'warning':
        logging.warning(message)

def safe_exit(code: int = 0, msg: str | None = None) -> None:
    if msg:
        log_and_print(msg, level='info')
    log_and_print("Done. Full log written to: " + log_filename, level='info')
    sys.exit(code)

# ---------- Helpers ----------
def parse_meraki_ts(ts: str) -> datetime:
    """
    Parse Meraki/RFC3339 timestamps like:
    2023-07-21T10:11:36Z / .625090Z / +00:00
    Returns an aware datetime in UTC.
    """
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

def chunked(iterable: List[Dict[str, Any]], size: int) -> Iterator[List[Dict[str, Any]]]:
    for i in range(0, len(iterable), size):
        yield iterable[i:i+size]

def matches_model_prefix(model: str, tokens: List[str]) -> bool:
    """
    Return True if model matches any token.
    - Plain tokens are treated as case-insensitive prefixes (e.g., 'MR', 'MX2').
    - '*' wildcard is supported; token 'MR4*' becomes regex ^MR4.*$
    """
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

# ---------- API Setup ----------
def validate_api_key(key: str) -> bool:
    # Allow upper/lower hex (Meraki keys are 40 chars)
    if not re.fullmatch(r'[A-Fa-f0-9]{40}', key):
        logging.error("Invalid API key format")
        return False
    return True

MAX_API_KEY_ATTEMPTS = 4
attempts = 0
API_KEY = ""
while attempts < MAX_API_KEY_ATTEMPTS:
    API_KEY = getpass("Enter your Meraki API key (hidden): ")
    if validate_api_key(API_KEY):
        break
    else:
        attempts += 1
        print(f"❌ This API key is invalid. Please double check and input the correct API key. ({MAX_API_KEY_ATTEMPTS - attempts} attempt(s) left)")
        logging.error(f"Invalid API key attempt {attempts}")
else:
    print("❌ Maximum attempts reached. Exiting.")
    sys.exit(1)

BASE_URL = "https://api.meraki.com/api/v1"
HEADERS = {
    "X-Cisco-Meraki-API-Key": API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# Requests Session with retries and backoff
session = requests.Session()
retry = Retry(
    total=8,
    backoff_factor=1.5,  # exponential: 1.5s, 3s, 4.5s, ...
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
    respect_retry_after_header=True
)
adapter = HTTPAdapter(max_retries=retry)
session.mount("https://", adapter)
session.mount("http://", adapter)

def log_rate_headers(resp: requests.Response) -> None:
    for h in ["X-Rate-Limit-Remaining", "X-Rate-Limit-Reset", "X-Request-Id"]:
        if h in resp.headers:
            logging.info(f"{h}: {resp.headers[h]}")

def api_request(method: str, endpoint: str,
                params: Dict[str, Any] | None = None,
                json: Dict[str, Any] | None = None,
                timeout: int = 30) -> Any:
    """
    Makes an API request with retry/backoff and gentle client-side throttling.
    Respects server Retry-After on 429.
    """
    url = f"{BASE_URL}{endpoint}"
    while True:
        resp = session.request(method, url, headers=HEADERS, params=params, json=json, timeout=timeout)
        log_rate_headers(resp)
        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", "2"))
            log_and_print(f"Hit rate limit. Waiting {wait}s then retrying {url}", level='warning')
            time.sleep(wait)
            continue
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            rate_rem = resp.headers.get("X-Rate-Limit-Remaining")
            rate_res = resp.headers.get("X-Rate-Limit-Reset")
            log_and_print(f"API error at {url}: {e} (remaining={rate_rem}, reset={rate_res})", level='error')
            logging.error(traceback.format_exc())
            raise
        time.sleep(0.15)
        if resp.status_code == 204 or not resp.content:
            return None
        return resp.json()

# ---------- Step 1: API and org selection ----------
logging.info("Script started")

orgs_any: Any = None
try:
    orgs_any = api_request('GET', '/organizations')
except Exception as e:
    log_and_print(f"Error fetching organizations: {e}", level='error')
    sys.exit(1)

if not isinstance(orgs_any, list) or not orgs_any:
    safe_exit(1, "No organizations returned or invalid response structure.")

orgs: List[Dict[str, Any]] = cast(List[Dict[str, Any]], orgs_any)

print("Organizations:")
for idx, org in enumerate(orgs, 1):
    print(f"{idx}. {org.get('name','<no name>')} (ID: {org.get('id','<no id>')})")

# --- Limited-attempt org selection with warning then exit ---
MAX_ORG_ATTEMPTS = 3  # total attempts
org_attempts = 0
orgid: str | None = None
while org_attempts < MAX_ORG_ATTEMPTS:
    try:
        choice_str = input("Select organization by number: ").strip()
        org_choice = int(choice_str)
        if 1 <= org_choice <= len(orgs):
            orgid = str(orgs[org_choice - 1]['id'])
            break
        else:
            org_attempts += 1
            if org_attempts == 1:
                log_and_print("Warning: invalid selection. Please choose a number from the list.", 'warning')
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
if orgid is None:
    safe_exit(1, "No valid organization selected. Exiting.")

# Ensure orgid is a string before proceeding
orgid_str: str = str(orgid)

# ---------- Step 2: Fetch inventory devices ----------
def get_all_inventory_devices(orgid: str) -> List[Dict[str, Any]]:
    devices: List[Dict[str, Any]] = []
    starting_after: str | None = None
    while True:
        params: Dict[str, Any] = {"perPage": 1000}
        if starting_after:
            params["startingAfter"] = starting_after
        resp = api_request('GET', f"/organizations/{orgid}/inventoryDevices", params=params)
        if not isinstance(resp, list):
            logging.error("Unexpected response when fetching inventory devices (not a list).")
            break
        devices.extend(resp)
        if len(resp) < 1000:
            break
        starting_after = resp[-1].get('serial')
        time.sleep(0.2)
    return devices

try:
    print("Retrieving inventory devices...")
    devices: List[Dict[str, Any]] = get_all_inventory_devices(orgid_str)
    log_and_print(f"Found {len(devices)} devices in inventory.")
except Exception as e:
    log_and_print(f"Error fetching inventory devices: {e}", level='error')
    sys.exit(1)

# ---------- Step 3: Filter by claimed date (optional) ----------
user_input = input("Show only devices claimed more than how many months ago? (press Enter to skip): ").strip()
months: int | None = None
if user_input:
    try:
        months = int(user_input)
    except ValueError:
        print("Invalid number. Showing all unassigned devices.")
        months = None

unassigned_devices: List[Dict[str, Any]] = []
cutoff_date: datetime | None = None
if months:
    cutoff_date = datetime.now(UTC) - timedelta(days=months * 30)

for device in devices:
    network_id = device.get('networkId')
    if network_id:
        continue

    if months and cutoff_date is not None:
        claimed_at = device.get('claimedAt')
        if not claimed_at:
            logging.warning(f"No claimedAt for device {device.get('serial','N/A')}")
            continue
        try:
            claimed_dt = parse_meraki_ts(str(claimed_at))
        except Exception:
            logging.warning(f"Date parsing failed for device {device.get('serial','N/A')}: {claimed_at}")
            continue
        if claimed_dt < cutoff_date:
            unassigned_devices.append(device)
    else:
        unassigned_devices.append(device)

log_and_print(
    f"Found {len(unassigned_devices)} unassigned devices" +
    (f" claimed more than {months} months ago." if months else ".")
)

# ---- EARLY EXIT #1 ----
if not unassigned_devices:
    safe_exit(0, "Note: there are no devices that fit the criteria (unassigned/age filter). Exiting without changes.")

# ---------- Step 3b: Model filter (optional, supports prefixes & wildcards) ----------
all_models = sorted(set([str(dev.get('model', 'N/A')) for dev in unassigned_devices]))
print(f"\nAvailable models in filtered inventory: {', '.join(all_models) if all_models else '(none)'}")
model_input = input(
    "Enter model(s) or prefixes (comma separated; e.g., MG, MR, MX2; wildcard * supported like MR4*). "
    "Leave blank for all: "
).strip().upper()

selected_tokens: List[str] = [m.strip() for m in model_input.split(",") if m.strip()] if model_input else []

filtered_devices: List[Dict[str, Any]] = []
for device in unassigned_devices:
    dev_model = str(device.get('model', ''))
    if matches_model_prefix(dev_model, selected_tokens):
        filtered_devices.append(device)

if selected_tokens:
    log_and_print(f"User selected model filters: {', '.join(sorted(selected_tokens))}")
log_and_print(f"Devices eligible for removal after model filter: {len(filtered_devices)}")

# ---- EARLY EXIT #2 ----
if not filtered_devices:
    safe_exit(0, "Note: there are no devices that fit the criteria for removal after model filtering. Exiting without changes.")

# ---------- Step 4: Export preview to CSV (optional) ----------
export = input("Export these devices to CSV? (y/n): ").lower()
if export == 'y':
    filename = f"meraki_unassigned_inventory_{orgid_str}.csv"
    if filtered_devices:
        keys = sorted(set().union(*(d.keys() for d in filtered_devices)))
        with open(filename, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(keys))
            writer.writeheader()
            writer.writerows(filtered_devices)
        log_and_print(f"Filtered inventory exported to {filename}")
    else:
        log_and_print("No devices to export.", level='warning')

# ---------- Step 5: Dry-Run Preview ----------
print("\n--- DRY RUN: Devices that WOULD be removed ---")
for device in filtered_devices:
    print(
        f"Model: {device.get('model', 'N/A')}, "
        f"Serial: {device.get('serial', 'N/A')}, "
        f"MAC: {device.get('mac', 'N/A')}, "
        f"Name: {device.get('name', 'N/A')}, "
        f"Claimed At: {device.get('claimedAt', 'N/A')}"
    )
log_and_print(f"Dry-run previewed {len(filtered_devices)} devices for removal.")
confirm = input("\nProceed with ACTUAL removal from the org? (y/n): ").strip().lower()

# ---------- Step 6: Removal and logging ----------
if confirm != 'y':
    safe_exit(0, "Cancelled by user. Exiting without changes.")

removal_log: List[Dict[str, str]] = []

def remove_devices_from_org(orgid: str, serials: List[str]) -> Any:
    endpoint = f"/organizations/{orgid}/inventory/release"
    payload: Dict[str, Any] = {"serials": serials}
    return api_request('POST', endpoint, json=payload)

print("\nRemoving devices...")
serials_all = [str(d.get('serial')) for d in filtered_devices if d.get('serial')]
for batch in chunked([{"serial": s} for s in serials_all], 100):
    try:
        to_send = [str(item["serial"]) for item in batch]
        remove_devices_from_org(orgid_str, to_send)
        removed_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
        index = {str(d.get('serial')): d for d in filtered_devices}
        for serial in to_send:
            dev = index.get(serial, {})
            mac = str(dev.get('mac', 'N/A'))
            dev_model = str(dev.get('model', 'N/A'))
            dev_name = str(dev.get('name', 'N/A'))
            msg = f"Removed {serial} (Model: {dev_model}, Name: {dev_name}, MAC: {mac}) at {removed_at}"
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
        logging.error(traceback.format_exc())
    time.sleep(0.5)

if removal_log:
    removal_filename = f"meraki_removed_inventory_{orgid_str}.csv"
    with open(removal_filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["serial", "mac", "model", "name", "removed_at"])
        writer.writeheader()
        writer.writerows(removal_log)
    log_and_print(f"\nRemoval log exported to {removal_filename}")
else:
    log_and_print("No devices were removed, so no removal log was created.")

safe_exit(0)
