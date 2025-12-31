# Created by J A Said
# Create a new network in the specsavers organisation
# Updated 20250818 - better logic around primary MX

import requests
import logging
import re
from datetime import datetime
from getpass import getpass
import csv
import os
import time
import signal
import sys
import ipaddress
from typing import Any, Dict, List, Optional, Tuple, Set

# =====================
# Config & Constants
# =====================
REQUEST_TIMEOUT = 30  # seconds
BASE_URL = "https://api.meraki.com/api/v1"
MAX_RETRIES = 5
EXCLUDED_VLANS = {100, 110, 210, 220, 230, 235, 240}

# Logging setup
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
logging.basicConfig(
    filename=f"meraki_script_{timestamp}.log",
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
CSV_LOGFILE = f"meraki_network_creation_{timestamp}.csv"

# =====================
# Utility: CSV audit log
# =====================
def log_change(
    event: str,
    details: str,
    *,
    username: Optional[str] = None,
    device_serial: Optional[str] = None,
    device_name: Optional[str] = None,
    misc: Optional[str] = None,
    org_id: Optional[str] = None,
    org_name: Optional[str] = None,
    network_id: Optional[str] = None,
    network_name: Optional[str] = None,
) -> None:
    file_exists = os.path.isfile(CSV_LOGFILE)
    with open(CSV_LOGFILE, mode='a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow([
                'timestamp', 'event', 'details', 'user',
                'device_serial', 'device_name', 'misc',
                'org_id', 'org_name', 'network_id', 'network_name'
            ])
        writer.writerow([
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            event,
            details,
            username or OPERATOR,
            device_serial or '',
            device_name or '',
            misc or '',
            org_id or '',
            org_name or '',
            network_id or '',
            network_name or ''
        ])

# =====================
# Prompts
# =====================
OPERATOR = input("Enter your name or initials for audit logs: ")
DRY_RUN = input("Run in dry-run mode? (yes/no): ").strip().lower() in {'yes', 'y'}
print(f"{'DRY RUN: ' if DRY_RUN else ''}Actions will {'not ' if DRY_RUN else ''}be executed.")

# =====================
# API auth
# =====================
def validate_api_key(key: str) -> bool:
    return bool(re.fullmatch(r'[A-Fa-f0-9]{40}', key or ''))

MAX_API_KEY_ATTEMPTS = 4
attempts = 0
API_KEY = None
while attempts < MAX_API_KEY_ATTEMPTS:
    API_KEY = getpass("Enter your Meraki API key (hidden): ")
    if validate_api_key(API_KEY):
        break
    attempts += 1
    print(f"❌ Invalid API key. ({MAX_API_KEY_ATTEMPTS - attempts} attempt(s) left)")
else:
    print("❌ Maximum attempts reached. Exiting.")
    raise SystemExit(1)

HEADERS = {
    "X-Cisco-Meraki-API-Key": API_KEY,
    "Content-Type": "application/json"
}

# Graceful abort
_aborted = False
def _handle_sigint(signum, frame):
    global _aborted
    _aborted = True
    print("\nReceived Ctrl+C — attempting graceful shutdown...")
    log_change('workflow_abort', 'User interrupted with SIGINT')
signal.signal(signal.SIGINT, _handle_sigint)

# =====================
# HTTP layer
# =====================
class MerakiAPIError(Exception):
    def __init__(self, status_code: int, text: str, json_body: Optional[Any], url: str):
        super().__init__(f"Meraki API error: {status_code} {text}")
        self.status_code = status_code
        self.text = text
        self.json_body = json_body
        self.url = url

def _request(method: str, path: str, *, params=None, json_data=None) -> Any:
    url = f"{BASE_URL}{path}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if method == 'GET':
                resp = requests.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
            elif method == 'POST':
                resp = requests.post(url, headers=HEADERS, json=json_data, timeout=REQUEST_TIMEOUT)
            elif method == 'PUT':
                resp = requests.put(url, headers=HEADERS, json=json_data, timeout=REQUEST_TIMEOUT)
            elif method == 'DELETE':
                resp = requests.delete(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            else:
                raise ValueError("Unknown HTTP method")

            if resp.status_code == 429:
                wait = min(2 ** (attempt - 1), 30)
                logging.warning(f"429 rate limit for {url}. Sleeping {wait}s and retrying...")
                time.sleep(wait)
                continue

            if not resp.ok:
                try:
                    body = resp.json()
                except Exception:
                    body = None
                logging.error(f"{method} {url} -> {resp.status_code} {resp.text}")
                raise MerakiAPIError(resp.status_code, resp.text, body, url)

            if resp.text:
                try:
                    return resp.json()
                except Exception:
                    return resp.text
            return None
        except MerakiAPIError:
            raise
        except Exception as e:
            if attempt == MAX_RETRIES:
                logging.exception(f"HTTP error for {url}: {e}")
                raise
            wait = min(2 ** attempt, 30)
            logging.warning(f"HTTP exception {e} for {url}. Retrying in {wait}s...")
            time.sleep(wait)

def meraki_get(path, params=None):
    return _request('GET', path, params=params)

def meraki_post(path, data=None):
    return _request('POST', path, json_data=data)

def meraki_put(path, data=None):
    return _request('PUT', path, json_data=data)

def do_action(func, *args, **kwargs):
    if DRY_RUN:
        logging.debug(f"DRY RUN: {func.__name__} args={args} kwargs={kwargs}")
        return None
    return func(*args, **kwargs)

# =====================
# Serial validation (provided)
# =====================
def prompt_and_validate_serials(org_id: str) -> List[str]:
    """
    Collect device serials from the user.
    - Accepts EITHER a single comma-separated line OR multiple lines (one per line).
    - Finish multi-line entry by pressing Enter on a blank line.
    - If nothing is entered:
        • After 2 blank attempts: ask whether to continue WITHOUT serials (return []).
        • Up to 4 total attempts; if still nothing and user declines, exit gracefully.
    - Validates format (XXXX-XXXX-XXXX), checks inventory, and claims to org if 404.
    - Deduplicates while preserving order.
    """
    MAX_ENTRY_ATTEMPTS = 4
    BLANK_PROMPT_THRESHOLD = 2
    MAX_SERIAL_ATTEMPTS = 4
    serial_pattern = re.compile(r"[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}")

    attempt = 0
    blank_attempts = 0
    while attempt < MAX_ENTRY_ATTEMPTS:
        print(
            "\nEnter device serials:\n"
            " • Paste a single comma-separated line (e.g. XXXX-XXXX-XXXX,YYYY-YYYY-YYYY)\n"
            " • OR type one serial per line; press Enter on a blank line to finish."
        )

        # Gather input (supports single-line or multi-line)
        entered_lines: List[str] = []
        while True:
            line = input("Serial(s): ").strip().upper()
            if line == "":
                # blank line ends multi-line entry
                break
            entered_lines.append(line)
            # If user pasted a comma-separated list, stop immediately to parse it
            if "," in line:
                break

        # Parse into candidate serials
        candidates: List[str] = []
        for ln in entered_lines:
            parts = [p.strip() for p in ln.split(",") if p.strip()]
            candidates.extend(parts)

        # If nothing entered this attempt
        if not candidates:
            attempt += 1
            blank_attempts += 1
            remaining = MAX_ENTRY_ATTEMPTS - attempt

            # After 2 blank tries, offer to continue without serials
            if blank_attempts >= BLANK_PROMPT_THRESHOLD:
                choice = input(
                    "No serial numbers entered.\n"
                    "Do you want to continue creating the network WITHOUT serials? (yes/no): "
                ).strip().lower()
                if choice in {"y", "yes"}:
                    print("Proceeding without serials.")
                    return []

            if remaining > 0:
                print(f"❌ No serial number entered. ({remaining} attempt(s) left)")
                continue
            else:
                print("No serial numbers provided after multiple attempts. Exiting gracefully.")
                raise SystemExit(1)

        # Deduplicate while preserving order
        seen: Set[str] = set()
        serial_list: List[str] = []
        for s in candidates:
            if s in seen:
                print(f"ℹ️  Duplicate serial '{s}' removed from input.")
                continue
            seen.add(s)
            serial_list.append(s)

        # Validate each serial and ensure present in org inventory (claim if 404)
        collected: List[str] = []
        for idx, original_serial in enumerate(serial_list, start=1):
            attempts_for_this = 0
            serial = original_serial
            while attempts_for_this < MAX_SERIAL_ATTEMPTS:
                if not serial_pattern.fullmatch(serial or ""):
                    attempts_for_this += 1
                    if attempts_for_this >= MAX_SERIAL_ATTEMPTS:
                        print(f"❌ Maximum attempts reached for serial #{idx} ({original_serial}). Skipping.")
                        break
                    serial = input(
                        f"Serial #{idx} '{serial}' is invalid. Re-enter (attempt {attempts_for_this+1}/{MAX_SERIAL_ATTEMPTS}): "
                    ).strip().upper()
                    continue

                # Inventory check
                try:
                    meraki_get(f"/organizations/{org_id}/inventoryDevices/{serial}")
                    print(f"✅ {serial} found in org inventory.")
                    collected.append(serial)
                    break
                except MerakiAPIError as e:
                    if getattr(e, "status_code", None) == 404:
                        # Claim into org
                        try:
                            do_action(meraki_post, f"/organizations/{org_id}/claim", data={"serials": [serial]})
                            print(f"✅ Serial '{serial}' successfully claimed into org inventory.")
                            log_change('device_claimed_inventory', "Claimed serial into org inventory", device_serial=serial)
                            collected.append(serial)
                            break
                        except Exception as claim_ex:
                            attempts_for_this += 1
                            print(f"❌ Error claiming '{serial}' into org inventory: {claim_ex}")
                            if attempts_for_this >= MAX_SERIAL_ATTEMPTS:
                                print(f"❌ Maximum attempts reached for serial #{idx}. Skipping.")
                                break
                            serial = input(
                                f"Re-enter serial #{idx} (attempt {attempts_for_this+1}/{MAX_SERIAL_ATTEMPTS}): "
                            ).strip().upper()
                            continue
                    else:
                        print(f"API Error for serial '{serial}': {e}")
                        break
                except Exception as e:
                    print(f"API Error for serial '{serial}': {e}")
                    break

        if collected:
            return collected

        # We got input but none validated/claimed this round
        attempt += 1
        remaining = MAX_ENTRY_ATTEMPTS - attempt
        if remaining > 0:
            print(f"⚠️  No valid serials collected. ({remaining} attempt(s) left)")
        else:
            print("No valid serials collected after multiple attempts. Exiting gracefully.")
            raise SystemExit(1)

    # Should not reach here; satisfies type checker
    return []

# =====================
# Network name + address
# =====================
def _slug_basic(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"[^a-z0-9\-]", "", s)
    s = re.sub(r"-{2,}", "-", s)
    return s.strip("-") or "site"

def prompt_network_identity_and_address() -> Tuple[str, str]:
    while True:
        cc = input("Country code (2 letters, e.g., UK): ").strip().upper()
        if re.fullmatch(r"[A-Z]{2}", cc):
            break
        print("❌ Please enter exactly 2 letters for the country code (e.g., UK, IE).")

    while True:
        epos = input("Store EPOS (digits, e.g., 1025): ").strip()
        epos_digits = re.sub(r"\D", "", epos)
        if epos_digits:
            epos = epos_digits
            break
        print("❌ Please enter digits only for Store EPOS (e.g., 1025).")

    raw_name = input("Name (e.g., 1sidtest): ").strip()
    name = _slug_basic(raw_name)

    site_address = input("Site address (free text, e.g., '1 High Street, London, UK'): ").strip()

    network_name = f"{cc}-{epos}-{name}"
    print(f"→ New network name will be: {network_name}")
    return network_name, site_address

# =====================
# Org / template helpers
# =====================
def list_organizations() -> List[Dict[str, Any]]:
    return meraki_get("/organizations")

def choose_org() -> Tuple[str, str]:
    orgs = list_organizations()
    if not orgs:
        print("⚠️  No organizations visible to this API key.")
        raise SystemExit(1)

    print("\nAvailable organizations:")
    for i, o in enumerate(orgs, 1):
        print(f"  {i:>2}. {o['name']} ({o['id']})")

    while True:
        pick = input("Pick an organization by number (press Enter to cancel): ").strip().lower()
        if pick in {"", "q", "quit", "cancel"}:
            print("No Organisation selected -----------  Please retry when Org is known *******")
            raise SystemExit(1)
        try:
            idx = int(pick)
            if 1 <= idx <= len(orgs):
                return orgs[idx-1]["id"], orgs[idx-1]["name"]
        except ValueError:
            pass
        print("Please enter a valid number, or press Enter to cancel.")

def list_templates(org_id: str) -> List[Dict[str, Any]]:
    return meraki_get(f"/organizations/{org_id}/configTemplates")

# =====================
# Duplicate name + subnets lookup
# =====================
def get_network_ip_subnets(network_id: str) -> List[str]:
    subnets: List[str] = []
    try:
        vlans = meraki_get(f"/networks/{network_id}/appliance/vlans") or []
        for v in vlans:
            cidr = v.get("subnet")
            if cidr:
                subnets.append(cidr)
        if subnets:
            return subnets
    except Exception:
        pass
    try:
        sl = meraki_get(f"/networks/{network_id}/appliance/singleLan") or {}
        cidr = sl.get("subnet")
        if cidr:
            subnets.append(cidr)
    except Exception:
        pass
    return subnets

def abort_if_network_exists(org_id: str, org_name: str, proposed_name: str) -> None:
    try:
        nets = meraki_get(f"/organizations/{org_id}/networks", params={"perPage": 1000}) or []
    except Exception as e:
        logging.warning(f"Failed to list networks for duplicate-name check: {e}")
        return

    target = proposed_name.strip().lower()
    for n in nets:
        existing_name = (n.get("name") or "").strip()
        if existing_name.lower() == target:
            existing_id = n.get("id", "?")
            subnets = []
            try:
                subnets = get_network_ip_subnets(existing_id)
            except Exception:
                pass

            print("\n⚠️  Network name already exists — aborting.")
            print(f"   • Organization : {org_name} ({org_id})")
            print(f"   • Network name : {existing_name}")
            print(f"   • Network ID   : {existing_id}")
            if subnets:
                print(f"   • Subnets      : {', '.join(subnets)}")
            else:
                print(f"   • Subnets      : (none found or not an MX network)")

            log_change(
                "network_name_conflict",
                f"Network '{existing_name}' already exists in org '{org_name}'",
                org_id=org_id,
                org_name=org_name,
                network_id=existing_id,
                network_name=existing_name,
                misc=f"subnets={subnets}" if subnets else "subnets=none",
            )
            raise SystemExit(1)

# =====================
# Partition helpers
# =====================
def get_bound_template_id(network_id: str) -> Optional[str]:
    try:
        net = meraki_get(f"/networks/{network_id}")
        return net.get("configTemplateId")
    except Exception:
        logging.exception("Failed to retrieve network for template binding info")
        return None

def get_network_devices_partitioned(network_id: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    devs = meraki_get(f"/networks/{network_id}/devices") or []
    mx_list = [d for d in devs if d.get("model","").upper().startswith("MX")]
    mr_list = [d for d in devs if d.get("model","").upper().startswith("MR") or d.get("model","").upper().startswith("CW916")]
    ms_list = [d for d in devs if d.get("model","").upper().startswith("MS")]
    return mx_list, mr_list, ms_list

# =====================
# Selection logic
# =====================
def select_primary_mx(org_id: str, serials: List[str]) -> Optional[str]:
    """
    If multiple MX are present, ask which should be PRIMARY (mx-01).
    If user presses Enter or types 'skip'/'cancel', auto-select the MX with the lowest serial.
    """
    mx_candidates: List[Tuple[str, str]] = []
    for s in serials:
        try:
            inv = meraki_get(f"/organizations/{org_id}/inventoryDevices/{s}")
            model = (inv.get('model') or '').upper()
            if model.startswith('MX'):
                mx_candidates.append((s, model))
        except Exception:
            logging.exception(f"Unable to read inventory for {s}")

    if len(mx_candidates) == 0:
        return None
    if len(mx_candidates) == 1:
        return mx_candidates[0][0]

    auto_choice = sorted([s for s, _ in mx_candidates])[0]

    print("\nMultiple MX devices detected in the claimed list:")
    for idx, (s, m) in enumerate(mx_candidates, 1):
        print(f" {idx}. {s}  ({m})")
    sel = input("Select which MX should be PRIMARY (mx-01). "
                "Enter number, or press Enter / type 'skip'/'cancel' to auto-select: ").strip().lower()

    if not sel or sel in {'skip', 'cancel'}:
        print(f"ℹ️  No explicit choice made. Auto-selecting PRIMARY MX: {auto_choice}")
        return auto_choice

    if sel.isdigit():
        i = int(sel)
        if 1 <= i <= len(mx_candidates):
            return mx_candidates[i-1][0]

    print(f"ℹ️  Invalid selection. Auto-selecting PRIMARY MX: {auto_choice}")
    return auto_choice

def select_device_order(org_id: str, serials: List[str], kind: str) -> List[str]:
    """
    Choose an explicit order for devices of a given type (MR/CW916 or MS).
    If user presses Enter / 'skip'/'cancel', auto-order by serial (alphanumeric).
    kind must be 'MR' or 'MS'.
    Returns ordered list of serials for that kind.
    """
    filtered: List[Tuple[str, str]] = []  # (serial, model)
    for s in serials:
        try:
            inv = meraki_get(f"/organizations/{org_id}/inventoryDevices/{s}")
            model = (inv.get('model') or '').upper()
            if kind == 'MR' and (model.startswith('MR') or model.startswith('CW916')):
                filtered.append((s, model))
            elif kind == 'MS' and model.startswith('MS'):
                filtered.append((s, model))
        except Exception:
            logging.exception(f"Unable to read inventory for {s}")

    if len(filtered) <= 1:
        return [s for s, _ in filtered]

    auto_order = sorted([s for s, _ in filtered])

    print(f"\nSelect ordering for {kind} devices (enter a comma-separated list of indices).")
    for idx, (s, m) in enumerate(filtered, 1):
        print(f" {idx}. {s}  ({m})")
    raw = input(f"Desired order for {kind} (e.g. 2,1,3). "
                "Press Enter / type 'skip'/'cancel' to auto-order: ").strip().lower()

    if not raw or raw in {'skip', 'cancel'}:
        print(f"ℹ️  Auto-ordering {kind} devices by serial: {', '.join(auto_order)}")
        return auto_order

    parts = [p.strip() for p in raw.split(',') if p.strip()]
    if all(p.isdigit() and 1 <= int(p) <= len(filtered) for p in parts) and len(parts) == len(filtered):
        return [filtered[int(p)-1][0] for p in parts]

    print(f"ℹ️  Invalid list. Auto-ordering {kind} devices by serial: {', '.join(auto_order)}")
    return auto_order

# =====================
# MX-aware template filtering
# =====================
def _claimed_mx_models(org_id: str, serials: List[str]) -> Set[str]:
    models: Set[str] = set()
    for s in serials:
        try:
            inv = meraki_get(f"/organizations/{org_id}/inventoryDevices/{s}") or {}
            model = (inv.get("model") or "").upper()
            if model.startswith("MX"):
                models.add(model)
        except Exception:
            logging.exception(f"Inventory lookup failed for {s}")
    return models

def choose_template_for_mx(org_id: str, mx_models: Set[str]) -> Optional[Dict[str, Any]]:
    """
    If mx_models provided, only show templates whose NAME ends with 'NoLegacy-<MXMODEL>'.
    Example: 'Corp-Branch-NoLegacy-MX67' for model MX67.
    If no models or no matches, fall back to showing all templates.
    """
    templates = list_templates(org_id)
    if not templates:
        print("ℹ️  No configuration templates in this org. Continuing without binding.")
        return None

    filtered = []
    if mx_models:
        wanted_suffixes = {f"nolegacy-{m.lower()}" for m in mx_models}
        for t in templates:
            name = (t.get("name") or "")
            if any(name.lower().endswith(sfx) for sfx in wanted_suffixes):
                filtered.append(t)

    show = filtered if filtered else templates
    title = "Matching templates" if filtered else "All templates (no MX match found)"
    print(f"\n{title}:")
    for i, t in enumerate(show, 1):
        print(f"  {i:>2}. {t['name']} ({t['id']})")
    print("  0. Do not bind to a template")
    while True:
        pick = input("Pick a template (0 to skip): ").strip()
        try:
            idx = int(pick)
            if idx == 0:
                return None
            if 1 <= idx <= len(show):
                return show[idx-1]
        except ValueError:
            pass
        print("Please enter a valid number.")

# =====================
# Switch profile filtering + selection (after claim)
# =====================
def _claimed_ms_models(org_id: str, serials: List[str]) -> Set[str]:
    models: Set[str] = set()
    for s in serials:
        try:
            inv = meraki_get(f"/organizations/{org_id}/inventoryDevices/{s}") or {}
            model = (inv.get("model") or "").upper()
            if model.startswith("MS"):
                models.add(model)
        except Exception:
            logging.exception(f"Inventory lookup failed for {s}")
    return models

def choose_switch_profile_for_template_filtered(org_id: str, template_id: Optional[str], ms_models_claimed: Set[str]) -> Optional[Tuple[str, Set[str], str]]:
    """
    Returns (profile_id, supported_models_set, profile_name) or None.
    Filters profiles to only those that support at least one of the claimed MS models.
    """
    if not template_id:
        return None
    if not ms_models_claimed:
        print("ℹ️  No MS devices claimed; skipping switch profile selection.")
        return None
    try:
        profiles = meraki_get(f"/organizations/{org_id}/configTemplates/{template_id}/switch/profiles") or []
    except Exception as e:
        logging.warning(f"Could not list switch profiles: {e}")
        return None

    def _profile_models(p: Dict[str, Any]) -> Set[str]:
        if isinstance(p.get("models"), list):
            return set(m.upper() for m in p["models"])
        m = p.get("model")
        return {m.upper()} if isinstance(m, str) and m else set()

    filtered = []
    for p in profiles:
        pm = _profile_models(p)
        if pm and (pm & ms_models_claimed):
            filtered.append((p, pm))

    if not filtered:
        print("ℹ️  No switch profiles match the claimed MS models; skipping assignment.")
        return None

    print("\nCompatible switch profiles for your claimed MS models:")
    for i, (p, pm) in enumerate(filtered, 1):
        pid = p.get("id") or p.get("switchProfileId") or ""
        pname = p.get("name") or p.get("switchProfileName") or ""
        print(f"  {i:>2}. {pname} [{pid}]  Models: {', '.join(sorted(pm))}")

    pick = input("Pick a switch profile number to apply to compatible MS (or Enter to skip): ").strip()
    if not pick:
        return None
    if pick.isdigit():
        idx = int(pick)
        if 1 <= idx <= len(filtered):
            p, pm = filtered[idx-1]
            pid = p.get("id") or p.get("switchProfileId")
            pname = p.get("name") or p.get("switchProfileName") or ""
            print(f"→ Selected switch profile: {pname}")
            return (pid, pm, pname)
    print("ℹ️  Invalid choice. Skipping switch profile assignment.")
    return None

# =====================
# Device naming (MX/MR/MS/MG)
# =====================
def name_and_configure_claimed_devices(
    org_id: str,
    network_id: str,
    network_name: str,
    site_address: str,
    serials: List[str],
    ms_switch_profile: Optional[Tuple[str, Set[str], str]] = None,  # (profile_id, supported_models, profile_name)
    primary_mx_serial: Optional[str] = None,
    mr_order: Optional[List[str]] = None,
    ms_order: Optional[List[str]] = None,
):
    """
    Renames and configures devices.
    - MX -> <cc-epos>-mx-XX (primary first if provided)
    - MR/CW916 -> <cc-epos>-ap-XX
    - MS -> <cc-epos>-ms-XX (apply profile to compatible models)
    - MG -> <cc-epos>-mg-XX
    Also sets 'address' for each device to 'site_address'.
    """
    prefix = '-'.join(network_name.split('-')[:2]).lower()
    counts = {'MX': 1, 'MR': 1, 'MS': 1, 'MG': 1}

    # Lookup models once
    inv_by_serial: Dict[str, Dict[str, Any]] = {}
    for s in serials:
        try:
            inv_by_serial[s] = meraki_get(f"/organizations/{org_id}/inventoryDevices/{s}")
        except Exception:
            logging.exception(f"Failed inventory lookup for {s}")
            inv_by_serial[s] = {}

    # Partition
    mx_serials = [s for s in serials if (inv_by_serial.get(s, {}).get('model') or '').upper().startswith('MX')]
    mr_serials = [s for s in serials if any((inv_by_serial.get(s, {}).get('model') or '').upper().startswith(p) for p in ('MR', 'CW916'))]
    ms_serials = [s for s in serials if (inv_by_serial.get(s, {}).get('model') or '').upper().startswith('MS')]
    mg_serials = [s for s in serials if (inv_by_serial.get(s, {}).get('model') or '').upper().startswith('MG')]

    # Apply orders
    if primary_mx_serial and primary_mx_serial in mx_serials:
        mx_serials = [primary_mx_serial] + [s for s in mx_serials if s != primary_mx_serial]
    if mr_order:
        mr_serials = [s for s in mr_order if s in mr_serials]
    if ms_order:
        ms_serials = [s for s in ms_order if s in ms_serials]

    # --- MX ---
    for s in mx_serials:
        mdl = (inv_by_serial.get(s, {}).get('model') or '')
        data: Dict[str, Any] = {'name': f"{prefix}-mx-{counts['MX']:02}", 'address': site_address}
        counts['MX'] += 1
        try:
            do_action(meraki_put, f"/devices/{s}", data=data)
            log_change('device_update', f"Renamed and set address {site_address} ({mdl})",
                       device_serial=s, device_name=data.get('name', ''))
        except Exception:
            logging.exception(f"Failed configuring {s} (MX)")

    # --- MR ---
    for s in mr_serials:
        mdl = (inv_by_serial.get(s, {}).get('model') or '')
        data: Dict[str, Any] = {'name': f"{prefix}-ap-{counts['MR']:02}", 'address': site_address}
        counts['MR'] += 1
        try:
            do_action(meraki_put, f"/devices/{s}", data=data)
            log_change('device_update', f"Renamed and set address {site_address} ({mdl})",
                       device_serial=s, device_name=data.get('name', ''))
        except Exception:
            logging.exception(f"Failed configuring {s} (MR)")

    # --- MS ---
    ms_profile_id = None
    ms_profile_supported: Set[str] = set()
    ms_profile_name = ""
    if ms_switch_profile:
        ms_profile_id, ms_profile_supported, ms_profile_name = ms_switch_profile

    for s in ms_serials:
        mdl = (inv_by_serial.get(s, {}).get('model') or '').upper()
        data: Dict[str, Any] = {'name': f"{prefix}-ms-{counts['MS']:02}", 'address': site_address}
        if ms_profile_id and mdl in ms_profile_supported:
            data['switchProfileId'] = ms_profile_id
        counts['MS'] += 1
        try:
            do_action(meraki_put, f"/devices/{s}", data=data)
            applied = f"applied profile {ms_profile_name}" if 'switchProfileId' in data else "no profile (incompatible)"
            log_change('device_update', f"Renamed, set address, {applied} ({mdl})",
                       device_serial=s, device_name=data.get('name', ''))
            if ms_profile_id and mdl not in ms_profile_supported:
                print(f"ℹ️  Skipped applying profile to {s} ({mdl}) — not in supported models for selected profile.")
        except Exception:
            logging.exception(f"Failed configuring {s} (MS)")

    # --- MG ---
    for s in mg_serials:
        mdl = (inv_by_serial.get(s, {}).get('model') or '')
        data: Dict[str, Any] = {'name': f"{prefix}-mg-{counts['MG']:02}", 'address': site_address}
        counts['MG'] += 1
        try:
            do_action(meraki_put, f"/devices/{s}", data=data)
            log_change('device_update', f"Renamed and set address {site_address} ({mdl})",
                       device_serial=s, device_name=data.get('name', ''))
        except Exception:
            logging.exception(f"Failed configuring {s} (MG)")

def remove_recently_added_tag(network_id: str):
    devs = meraki_get(f"/networks/{network_id}/devices") or []
    for d in devs:
        tags = d.get('tags', [])
        if not isinstance(tags, list):
            tags = (tags or '').split()
        if 'recently-added' in tags:
            updated_tags = [t for t in tags if t != 'recently-added']
            print(f"Removing 'recently-added' tag from {d.get('model','?')} {d.get('serial','?')}")
            try:
                do_action(meraki_put, f"/devices/{d['serial']}", data={"tags": updated_tags})
                log_change('tag_removed', "Removed 'recently-added' tag",
                           device_serial=d['serial'], device_name=d.get('name', ''),
                           misc=f"old_tags={tags}, new_tags={updated_tags}")
            except Exception:
                logging.exception(f"Failed to remove 'recently-added' from {d.get('serial','?')}")

# =====================
# Switch stack creation (UPPERCASE CC-EPOS-STK-0x)
# =====================
def create_switch_stack_if_possible(network_id: str, org_id: str, network_name: str, serials: List[str]) -> None:
    import re as _re
    ms_serials = []
    for s in serials:
        try:
            rec = meraki_get(f"/organizations/{org_id}/inventoryDevices/{s}")
            if (rec.get("model", "") or "").upper().startswith("MS"):
                ms_serials.append((s, (rec.get("model", "") or "").upper()))
        except Exception:
            logging.exception(f"Inventory lookup failed for {s}")
    if len(ms_serials) < 2:
        return
    by_model: Dict[str, List[str]] = {}
    for s, m in ms_serials:
        by_model.setdefault(m, []).append(s)
    target_model = max(by_model, key=lambda m: len(by_model[m]))
    stack_serials = by_model[target_model]
    if len(stack_serials) < 2:
        return
    parts = (network_name or "").split("-")
    cc = (parts[0] if len(parts) > 0 else "XX").upper()
    epos = (parts[1] if len(parts) > 1 else "0000").upper()
    existing = meraki_get(f"/networks/{network_id}/switch/stacks") or []
    pat = _re.compile(rf"^{_re.escape(cc)}-{_re.escape(epos)}-STK-(\d+)$", _re.IGNORECASE)
    max_num = 0
    for st in existing:
        name = st.get("name", "")
        m = pat.match(name)
        if m:
            try:
                n = int(m.group(1))
                if n > max_num:
                    max_num = n
            except ValueError:
                continue
    next_num = max_num + 1
    stack_name = f"{cc}-{epos}-STK-{next_num:02d}"
    do_action(
        meraki_post,
        f"/networks/{network_id}/switch/stacks",
        data={"name": stack_name, "serials": stack_serials},
    )
    log_change(
        "switch_stack_created",
        f"Created stack for model {target_model}",
        network_id=network_id,
        misc=",".join(stack_serials),
    )
    print(f"✅ Switch stack created: {stack_name} ({len(stack_serials)} switches)")

# =====================
# Warm spare enforcement (swap if needed)
# =====================
def ensure_warm_spare_primary(network_id: str, primary_mx_serial: Optional[str]) -> None:
    """
    If warm spare is enabled and the selected PRIMARY MX isn't the current primary,
    call the swap endpoint to make it primary.
    """
    if not primary_mx_serial:
        return
    try:
        ws = meraki_get(f"/networks/{network_id}/appliance/warmSpare") or {}
    except MerakiAPIError as e:
        logging.warning(f"Warm spare lookup failed: {e.text}")
        return
    except Exception as e:
        logging.warning(f"Warm spare lookup exception: {e}")
        return

    enabled = ws.get("enabled")
    curr_primary = ws.get("primarySerial")
    curr_spare = ws.get("spareSerial")

    if not enabled:
        logging.info("Warm spare not enabled; nothing to swap.")
        return

    if curr_primary == primary_mx_serial:
        logging.info("Warm spare primary already correct.")
        return

    if primary_mx_serial not in {curr_primary, curr_spare}:
        print(f"ℹ️  Selected PRIMARY MX {primary_mx_serial} is not one of the warm spare pair "
              f"(primary={curr_primary}, spare={curr_spare}). Skipping swap.")
        return

    try:
        do_action(meraki_post, f"/networks/{network_id}/appliance/warmSpare/swap")
        log_change(
            'warm_spare_swapped',
            f"Swapped warm spare to make {primary_mx_serial} primary",
            network_id=network_id,
            device_serial=primary_mx_serial
        )
        print("✅ Warm spare roles swapped so the selected MX is primary.")
    except Exception as e:
        logging.exception(f"Failed to swap warm spare: {e}")
        print(f"❌ Failed to swap warm spare: {e}")

# =====================
# Org-wide subnet index & /23 planning with collision checks
# =====================
def get_org_subnets_index(org_id: str) -> List[Tuple[ipaddress.IPv4Network, str, str]]:
    index: List[Tuple[ipaddress.IPv4Network, str, str]] = []
    nets = meraki_get(f"/organizations/{org_id}/networks", params={"perPage": 1000}) or []
    for n in nets:
        nid = n.get("id")
        nname = n.get("name", "")
        try:
            for cidr in get_network_ip_subnets(nid):
                try:
                    net_cidr = ipaddress.IPv4Network(cidr, strict=True)  # IPv4-only
                    index.append((net_cidr, str(nname), str(nid)))
                except Exception:
                    # skip invalid/IPv6 entries
                    continue
        except Exception:
            continue
    return index

def derive_site23_standard_subnets(site23: ipaddress.IPv4Network) -> Dict[int, ipaddress.IPv4Network]:
    o1, o2, o3, _ = [int(x) for x in str(site23.network_address).split(".")]
    even24 = ipaddress.IPv4Network(f"{o1}.{o2}.{o3}.0/24", strict=True)
    vlan50 = ipaddress.IPv4Network(f"{o1}.{o2}.{o3+1}.192/27", strict=True)
    vlan10 = ipaddress.IPv4Network(f"{o1}.{o2}.{o3+1}.224/27", strict=True)
    return {40: even24, 50: vlan50, 10: vlan10}

def find_overlaps_in_org(candidate: ipaddress.IPv4Network,
                         org_index: List[Tuple[ipaddress.IPv4Network, str, str]]
                         ) -> List[Tuple[str, str, ipaddress.IPv4Network]]:
    out = []
    for sub, name, nid in org_index:
        if candidate.overlaps(sub):
            out.append((name, nid, sub))
    return out

def prompt_site_supernet_23() -> ipaddress.IPv4Network:
    while True:
        raw = input("Site IPv4 /23 (e.g., 10.x.y.0/23 where y is even): ").strip()
        try:
            net = ipaddress.IPv4Network(raw, strict=True)  # IPv4-only
            if net.prefixlen != 23:
                print("❌ Prefix must be /23.")
                continue
            if int(str(net.network_address).split(".")[0]) != 10:
                print("❌ Network must be within 10.0.0.0/8.")
                continue
            third_octet = int(str(net.network_address).split(".")[2])
            if third_octet % 2 != 0:
                print("❌ For a /23 boundary, the 3rd octet must be EVEN (e.g., ...18.0/23).")
                continue
            return net
        except Exception:
            print("❌ Invalid /23. Example: 10.25.18.0/23")

def prompt_vlan40_gateway_choice(even24: ipaddress.IPv4Network) -> int:
    while True:
        choice = input("VLAN 40 gateway should be .1 or .99? (enter 1 or 99, default 99): ").strip() or "99"
        if choice not in {"1", "99"}:
            print("❌ Please enter 1 or 99.")
            continue
        last = int(choice)
        gw_candidate = str(ipaddress.IPv4Address(int(even24.network_address) + last))
        confirm = input(f"Use {gw_candidate} as VLAN 40 gateway? (yes/no): ").strip().lower()
        if confirm in {"y", "yes"}:
            return last
        print("Okay, let's choose again.")

def prompt_site23_not_in_use(org_id: str, org_name: str) -> Tuple[ipaddress.IPv4Network, int]:
    org_index = get_org_subnets_index(org_id)
    while True:
        site23 = prompt_site_supernet_23()
        plan = derive_site23_standard_subnets(site23)
        conflicts: List[Tuple[int, ipaddress.IPv4Network, List[Tuple[str, str, ipaddress.IPv4Network]]]] = []
        for vid, subnet in plan.items():
            overlaps = find_overlaps_in_org(subnet, org_index)
            if overlaps:
                conflicts.append((vid, subnet, overlaps))
        if conflicts:
            print("\n⚠️  The proposed /23 produces subnets that overlap existing networks in this organization:")
            for vid, subnet, overlaps in conflicts:
                print(f"  • VLAN {vid}: {subnet}")
                for name, nid, osub in overlaps:
                    print(f"      ↳ overlaps {osub} in network '{name}' ({nid})")
            print("Please enter a different /23 and I’ll check again.\n")
            continue
        even24 = plan[40]
        vlan40_gw_last = prompt_vlan40_gateway_choice(even24)
        return site23, vlan40_gw_last

def _ip_is_usable_in(ip: ipaddress.IPv4Address, net: ipaddress.IPv4Network) -> bool:
    return ip in net and ip != net.network_address and ip != net.broadcast_address

def prompt_subnet_and_gateway_for_vlan(
    vlan_id: int,
    site23: ipaddress.IPv4Network,
    taken: List[ipaddress.IPv4Network],
    org_index: List[Tuple[ipaddress.IPv4Network, str, str]]
) -> Optional[Tuple[str, str]]:
    while True:
        raw_subnet = input(f"VLAN {vlan_id} subnet CIDR within {site23} (ENTER to skip): ").strip()
        if raw_subnet == "":
            return None
        try:
            subnet = ipaddress.IPv4Network(raw_subnet, strict=True)  # IPv4-only
            if not subnet.subnet_of(site23):
                print(f"❌ Subnet must be an IPv4 CIDR within {site23}.")
                continue
            if any(subnet.overlaps(t) for t in taken):
                print("❌ Subnet overlaps one already assigned in this site plan.")
                continue
            org_hits = find_overlaps_in_org(subnet, org_index)
            if org_hits:
                print("❌ Subnet overlaps existing network(s) in this organization:")
                for name, nid, osub in org_hits:
                    print(f"   ↳ {subnet} overlaps {osub} in '{name}' ({nid})")
                print("Please choose a different subnet.")
                continue
        except Exception:
            print("❌ Invalid CIDR. Example: 10.x.y.z/26")
            continue

        gw_raw = input(f"VLAN {vlan_id} gateway IP (must be inside {subnet}): ").strip()
        try:
            gw_ip = ipaddress.IPv4Address(gw_raw)  # IPv4-only
            if not _ip_is_usable_in(gw_ip, subnet):
                print("❌ Gateway must be a usable host IP inside the subnet.")
                continue
        except Exception:
            print("❌ Invalid IPv4 address.")
            continue

        ok = input(f"Apply VLAN {vlan_id}: subnet {subnet}, gateway {gw_ip}? (yes/no): ").strip().lower()
        if ok in {"y", "yes"}:
            taken.append(subnet)
            return (str(subnet), str(gw_ip))

def apply_site23_vlan_scheme(
    network_id: str,
    site23: ipaddress.IPv4Network,
    vlan40_gw_last_octet: int,
    excluded_vlans: Optional[set] = None,
    org_index: Optional[List[Tuple[ipaddress.IPv4Network, str, str]]] = None
) -> None:
    excluded_vlans = excluded_vlans or set()
    org_index = org_index or []
    try:
        do_action(meraki_put, f"/networks/{network_id}/appliance/vlanSettings", data={"vlansEnabled": True})
    except Exception as e:
        logging.warning(f"Could not enable VLANs (template-controlled or not MX?): {e}")
    o1, o2, o3, _ = [int(x) for x in str(site23.network_address).split(".")]
    even24 = ipaddress.IPv4Network(f"{o1}.{o2}.{o3}.0/24", strict=True)
    planned: Dict[int, Dict[str, str]] = {
        40: {"subnet": str(even24),
             "applianceIp": str(ipaddress.IPv4Address(int(even24.network_address) + vlan40_gw_last_octet)),
             "desc": "VLAN 40 (even /24)"},
        50: {"subnet": str(ipaddress.IPv4Network(f"{o1}.{o2}.{o3+1}.192/27", strict=True)),
             "applianceIp": f"{o1}.{o2}.{o3+1}.222",
             "desc": "VLAN 50 (odd .192/27)"},
        10: {"subnet": str(ipaddress.IPv4Network(f"{o1}.{o2}.{o3+1}.224/27", strict=True)),
             "applianceIp": f"{o1}.{o2}.{o3+1}.254",
             "desc": "VLAN 10 (odd .224/27)"},
    }
    taken_subnets: List[ipaddress.IPv4Network] = [
        ipaddress.IPv4Network(planned[40]["subnet"], strict=True),
        ipaddress.IPv4Network(planned[50]["subnet"], strict=True),
        ipaddress.IPv4Network(planned[10]["subnet"], strict=True),
    ]
    try:
        vlans = meraki_get(f"/networks/{network_id}/appliance/vlans") or []
    except Exception as e:
        print(f"❌ Failed to list VLANs: {e}")
        return
    updated = 0
    for v in vlans:
        raw_id = v.get("id", v.get("vlanId"))
        try:
            vid = int(str(raw_id))
        except Exception:
            logging.warning(f"Skipping VLAN with non-numeric id: {raw_id}")
            continue
        if vid in EXCLUDED_VLANS:
            print(f"⏭️  Skipping excluded VLAN {vid}")
            continue
        if vid in planned:
            payload = {"subnet": planned[vid]["subnet"], "applianceIp": planned[vid]["applianceIp"]}
            try:
                do_action(meraki_put, f"/networks/{network_id}/appliance/vlans/{vid}", data=payload)
                log_change("vlan_updated", f"{planned[vid]['desc']}: {payload['subnet']} gw {payload['applianceIp']}",
                           network_id=network_id, misc=str(payload))
                print(f"✅ VLAN {vid} updated → {payload['subnet']} (gw {payload['applianceIp']})")
                updated += 1
            except Exception as e:
                logging.exception(f"Failed to update VLAN {vid}: {e}")
                print(f"❌ Failed to update VLAN {vid}: {e}")
    for v in vlans:
        raw_id = v.get("id", v.get("vlanId"))
        try:
            vid = int(str(raw_id))
        except Exception:
            continue
        if vid in EXCLUDED_VLANS or vid in planned:
            continue
        res = prompt_subnet_and_gateway_for_vlan(vid, site23, taken_subnets, org_index)
        if not res:
            print(f"↪️  Skipped VLAN {vid}.")
            continue
        subnet_str, gw_ip_str = res
        payload = {"subnet": subnet_str, "applianceIp": gw_ip_str}
        try:
            do_action(meraki_put, f"/networks/{network_id}/appliance/vlans/{vid}", data=payload)
            log_change("vlan_updated", f"Manual VLAN {vid}: {payload['subnet']} gw {payload['applianceIp']}",
                       network_id=network_id, misc=str(payload))
            print(f"✅ VLAN {vid} updated → {payload['subnet']} (gw {payload['applianceIp']})")
            updated += 1
        except Exception as e:
            logging.exception(f"Failed to update VLAN {vid}: {e}")
            print(f"❌ Failed to update VLAN {vid}: {e}")
    if updated == 0:
        print("ℹ️  No VLANs updated (none found or all excluded).")

def handle_vlan_plan_prompt(org_id: str, org_name: str, network_id: str):
    resp = input("Configure standard site /23 for VLANs 40/50/10 now? Type 'yes' or paste a /23 (e.g., 10.192.20.0/23): ").strip()
    cidr_re = r"\d+\.\d+\.\d+\.\d+/\d{1,2}"
    want = resp.lower() in {"y", "yes"} or bool(re.fullmatch(cidr_re, resp))
    if not want:
        return
    org_index = get_org_subnets_index(org_id)
    if re.fullmatch(cidr_re, resp):
        try:
            primed = ipaddress.IPv4Network(resp, strict=True)  # IPv4-only
        except Exception:
            primed = None
        ok = False
        if primed and primed.prefixlen == 23 and int(str(primed.network_address).split(".")[0]) == 10 and int(str(primed.network_address).split(".")[2]) % 2 == 0:
            plan = derive_site23_standard_subnets(primed)
            conflicts = []
            for vid, sn in plan.items():
                hits = find_overlaps_in_org(sn, org_index)
                if hits:
                    conflicts.append((vid, sn, hits))
            if conflicts:
                print("\n⚠️  The /23 you entered overlaps existing networks:")
                for vid, sn, hits in conflicts:
                    print(f"  • VLAN {vid}: {sn}")
                    for name, nid, osub in hits:
                        print(f"      ↳ overlaps {osub} in '{name}' ({nid})")
                print("Falling back to guided prompts.\n")
            else:
                vlan40_gw_last = prompt_vlan40_gateway_choice(plan[40])
                apply_site23_vlan_scheme(
                    network_id=network_id,
                    site23=primed,
                    vlan40_gw_last_octet=vlan40_gw_last,
                    excluded_vlans=EXCLUDED_VLANS,
                    org_index=org_index,
                )
                ok = True
        if ok:
            return
    site23, vlan40_gw_last = prompt_site23_not_in_use(org_id, org_name)
    apply_site23_vlan_scheme(
        network_id=network_id,
        site23=site23,
        vlan40_gw_last_octet=vlan40_gw_last,
        excluded_vlans=EXCLUDED_VLANS,
        org_index=get_org_subnets_index(org_id),
    )

# =====================
# Main
# =====================
def main():
    if _aborted:
        return

    org_id, org_name = choose_org()
    network_name, site_address = prompt_network_identity_and_address()

    # Duplicate-name guard
    abort_if_network_exists(org_id, org_name, network_name)

    # Enter serials now (so we can filter templates by MX model)
    serials = prompt_and_validate_serials(org_id)

    # Determine MX-aware filtered template list
    mx_models = _claimed_mx_models(org_id, serials)
    tmpl = choose_template_for_mx(org_id, mx_models)  # may be None

    # Create network
    tz = "Europe/London"
    product_types = ["appliance", "switch", "wireless", "cellularGateway"]
    net = do_action(meraki_post, f"/organizations/{org_id}/networks", data={
        "name": network_name,
        "productTypes": product_types,
        "timeZone": tz
    }) or {"id": "DRYRUNID", "name": network_name}
    network_id = net["id"]
    log_change("network_created", f"Created network '{network_name}' with {product_types}", org_id=org_id, network_id=network_id, network_name=network_name)

    # Store site address in network notes
    try:
        do_action(meraki_put, f"/networks/{network_id}", data={"notes": f"Site address: {site_address}"})
        log_change("network_notes_updated", f"Stored site address in notes: {site_address}", network_id=network_id)
    except Exception as e:
        logging.warning(f"Could not update network notes with address: {e}")

    # Optional: bind to template
    template_id = None
    if tmpl:
        try:
            do_action(meraki_post, f"/networks/{network_id}/bind", data={"configTemplateId": tmpl["id"], "autoBind": True})
            log_change("network_bound_template", f"Bound to template {tmpl['name']}", network_id=network_id)
            template_id = tmpl["id"]
        except MerakiAPIError as e:
            print(f"⚠️  Template bind failed: {e.text}")
            logging.warning(f"Template bind failed: {e.text}")
            template_id = None

    # VLAN config (accepts yes or pasted /23)
    handle_vlan_plan_prompt(org_id, org_name, network_id)

    # Device ordering & primary MX choice
    primary_mx_serial = select_primary_mx(org_id, serials) if serials else None
    mr_order = select_device_order(org_id, serials, 'MR') if serials else []
    ms_order = select_device_order(org_id, serials, 'MS') if serials else []

    # Claim devices (order doesn't matter; warm spare swap will enforce PRIMARY)
    if serials:
        do_action(meraki_post, f"/networks/{network_id}/devices/claim", data={"serials": serials})
        for s in serials:
            log_change("device_claimed_network", "Claimed device into network", device_serial=s, network_id=network_id)

        # Filtered switch profile selection (AFTER claim)
        selected_switch_profile: Optional[Tuple[str, Set[str], str]] = None
        if template_id:
            ms_models = _claimed_ms_models(org_id, serials)
            selected_switch_profile = choose_switch_profile_for_template_filtered(org_id, template_id, ms_models)

        # Name and configure devices (incl. addresses + optional MS profile for compatible models)
        name_and_configure_claimed_devices(
            org_id=org_id,
            network_id=network_id,
            network_name=network_name,
            site_address=site_address,
            serials=serials,
            ms_switch_profile=selected_switch_profile,
            primary_mx_serial=primary_mx_serial,  # used for mx-01 naming
            mr_order=mr_order,
            ms_order=ms_order,
        )

        # Create a switch stack if possible
        create_switch_stack_if_possible(network_id, org_id, network_name, serials)

        # Ensure warm spare primary matches selected MX01
        ensure_warm_spare_primary(network_id, primary_mx_serial)

        # Cleanup: always remove the 'recently-added' tag if present
        remove_recently_added_tag(network_id)

    print("\n✅ Workflow complete.")
    log_change("workflow_complete", "Network creation workflow finished", org_id=org_id, org_name=org_name, network_id=network_id, network_name=network_name)

if __name__ == "__main__":
    try:
        main()
    except MerakiAPIError as e:
        print(f"Meraki API error: {e.status_code} — {e.text}\nURL: {e.url}")
        log_change("error_meraki_api", f"{e.status_code} {e.text}", misc=e.url)
        sys.exit(2)
    except SystemExit:
        raise
    except Exception as e:
        logging.exception("Unhandled exception in main()")
        print(f"Unexpected error: {e}")
        log_change("error_unhandled", str(e))
        sys.exit(3)

