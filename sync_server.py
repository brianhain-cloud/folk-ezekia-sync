"""
Folk-Ezekia Bidirectional Sync Server
Deployed on Railway - Syncs data between Folk CRM and Ezekia

Direction 1: Ezekia → Folk (via Zapier webhook triggers)
Direction 2: Folk → Ezekia (via polling + Zapier webhook actions)
"""

from flask import Flask, request, jsonify
import requests
import os
import json
import hashlib
from datetime import datetime, timedelta

app = Flask(__name__)

# Configuration from environment variables
FOLK_API_KEY = os.environ.get('FOLK_API_KEY', '')
FOLK_BASE_URL = "https://api.folk.app/v1"
PORT = int(os.environ.get('PORT', 5001))

# Zapier webhook URLs for Folk → Ezekia sync
ZAPIER_PERSON_NEW_URL = os.environ.get('ZAPIER_PERSON_NEW_URL', '')
ZAPIER_PERSON_UPDATE_URL = os.environ.get('ZAPIER_PERSON_UPDATE_URL', '')
ZAPIER_COMPANY_NEW_URL = os.environ.get('ZAPIER_COMPANY_NEW_URL', '')
ZAPIER_COMPANY_UPDATE_URL = os.environ.get('ZAPIER_COMPANY_UPDATE_URL', '')

# State file for tracking synced records
STATE_FILE = os.environ.get('STATE_FILE', '/tmp/sync_state.json')

# Cooldown period to prevent sync loops (in seconds)
SYNC_COOLDOWN = 300  # 5 minutes


class FolkClient:
    """Client for interacting with Folk CRM API"""

    def __init__(self, api_key):
        self.api_key = api_key
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

    def test_connection(self):
        """Test API connectivity"""
        response = requests.get(f"{FOLK_BASE_URL}/groups", headers=self.headers)
        return response.status_code == 200

    # ==================== PEOPLE ====================

    def list_people(self, limit=100, cursor=None):
        """List all people from Folk with pagination"""
        params = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        response = requests.get(f"{FOLK_BASE_URL}/people", headers=self.headers, params=params)
        print(f"[Folk API] List people: {response.status_code}")
        if response.status_code == 200:
            return response.json()
        return None

    def get_all_people(self):
        """Fetch all people using pagination"""
        all_people = []
        cursor = None
        while True:
            result = self.list_people(limit=100, cursor=cursor)
            if not result:
                break
            # Folk API returns 'items' - could be dict (id->person) or list
            items = result.get('items', result.get('data', []))
            if not items:
                break
            # Handle both dict and list formats
            if isinstance(items, dict):
                all_people.extend(items.values())
            else:
                all_people.extend(items)
            # Check pagination for next cursor
            pagination = result.get('pagination', {})
            cursor = pagination.get('nextCursor') or result.get('nextCursor')
            if not cursor:
                break
        return all_people

    def get_person(self, person_id):
        """Get a single person by ID"""
        response = requests.get(f"{FOLK_BASE_URL}/people/{person_id}", headers=self.headers)
        if response.status_code == 200:
            return response.json()
        return None

    def create_person(self, person_data):
        """Create a new person in Folk"""
        print(f"[Folk API] Creating person: {person_data}")
        response = requests.post(
            f"{FOLK_BASE_URL}/people",
            headers=self.headers,
            json=person_data
        )
        print(f"[Folk API] Response: {response.status_code} - {response.text[:200]}")
        return response

    def update_person(self, person_id, person_data):
        """Update an existing person in Folk"""
        print(f"[Folk API] Updating person {person_id}: {person_data}")
        response = requests.patch(
            f"{FOLK_BASE_URL}/people/{person_id}",
            headers=self.headers,
            json=person_data
        )
        print(f"[Folk API] Response: {response.status_code}")
        return response

    def search_person_by_email(self, email):
        """Search for a person by email address"""
        if not email:
            return None
        people = self.get_all_people()
        for person in people:
            if not isinstance(person, dict):
                continue
            emails = person.get('emails', [])
            for e in emails:
                # Handle both string format and object format
                email_val = e if isinstance(e, str) else e.get('value', '')
                if email_val.lower() == email.lower():
                    return person
        return None

    def search_person_by_name(self, first_name, last_name):
        """Search for a person by first and last name"""
        if not first_name and not last_name:
            return None
        people = self.get_all_people()
        for person in people:
            if not isinstance(person, dict):
                continue
            fn = person.get('firstName', '').lower()
            ln = person.get('lastName', '').lower()
            if fn == first_name.lower() and ln == last_name.lower():
                return person
        return None

    # ==================== GROUPS ====================

    def list_groups(self):
        """List all groups from Folk"""
        response = requests.get(f"{FOLK_BASE_URL}/groups", headers=self.headers)
        print(f"[Folk API] List groups: {response.status_code}")
        if response.status_code == 200:
            return response.json()
        return None

    def get_first_group_id(self):
        """Get the ID of the first group (for custom fields)"""
        result = self.list_groups()
        if result:
            items = result.get('items', result.get('data', []))
            if isinstance(items, dict) and items:
                # Dict format: get first key
                return list(items.keys())[0]
            elif isinstance(items, list) and items:
                # List format: get first item's ID
                return items[0].get('id', '')
        return None

    # ==================== CUSTOM FIELDS ====================

    def update_person_custom_field(self, person_id, group_id, field_name, value):
        """Update a custom field value on a person"""
        update_data = {
            "customFieldValues": {
                group_id: {
                    field_name: value
                }
            }
        }
        print(f"[Folk API] Updating custom field '{field_name}' on person {person_id}")
        return self.update_person(person_id, update_data)

    def append_to_custom_field(self, person_id, group_id, field_name, new_content, timestamp=True):
        """Append content to a text custom field (for notes history)"""
        # First get current value
        person = self.get_person(person_id)
        if not person:
            return None

        current_value = ""
        custom_fields = person.get('customFieldValues', {})
        if group_id in custom_fields:
            current_value = custom_fields[group_id].get(field_name, '') or ''

        # Build new value with timestamp
        if timestamp:
            ts = datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')
            new_entry = f"[{ts}] {new_content}"
        else:
            new_entry = new_content

        # Append to existing (newest first)
        if current_value:
            updated_value = f"{new_entry}\n---\n{current_value}"
        else:
            updated_value = new_entry

        return self.update_person_custom_field(person_id, group_id, field_name, updated_value)

    # ==================== COMPANIES ====================

    def list_companies(self, limit=100, cursor=None):
        """List all companies from Folk with pagination"""
        params = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        response = requests.get(f"{FOLK_BASE_URL}/companies", headers=self.headers, params=params)
        print(f"[Folk API] List companies: {response.status_code}")
        if response.status_code == 200:
            return response.json()
        return None

    def get_all_companies(self):
        """Fetch all companies using pagination"""
        all_companies = []
        cursor = None
        while True:
            result = self.list_companies(limit=100, cursor=cursor)
            if not result:
                break
            # Folk API returns 'items' - could be dict (id->company) or list
            items = result.get('items', result.get('data', []))
            if not items:
                break
            # Handle both dict and list formats
            if isinstance(items, dict):
                all_companies.extend(items.values())
            else:
                all_companies.extend(items)
            # Check pagination for next cursor
            pagination = result.get('pagination', {})
            cursor = pagination.get('nextCursor') or result.get('nextCursor')
            if not cursor:
                break
        return all_companies

    def get_company(self, company_id):
        """Get a single company by ID"""
        response = requests.get(f"{FOLK_BASE_URL}/companies/{company_id}", headers=self.headers)
        if response.status_code == 200:
            return response.json()
        return None

    def create_company(self, company_data):
        """Create a new company in Folk"""
        print(f"[Folk API] Creating company: {company_data}")
        response = requests.post(
            f"{FOLK_BASE_URL}/companies",
            headers=self.headers,
            json=company_data
        )
        print(f"[Folk API] Response: {response.status_code} - {response.text[:200]}")
        return response


# Initialize Folk client
folk_client = FolkClient(FOLK_API_KEY)


# ==================== STATE MANAGEMENT ====================

def load_state():
    """Load sync state from file"""
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"[State] Error loading state: {e}")
    return {
        "last_poll": None,
        "people": {},
        "companies": {},
        "recent_syncs": {}  # Track recent syncs to prevent loops
    }


def save_state(state):
    """Save sync state to file"""
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2, default=str)
        print(f"[State] Saved state to {STATE_FILE}")
    except Exception as e:
        print(f"[State] Error saving state: {e}")


def compute_hash(data):
    """Compute hash of record data for change detection"""
    # Normalize and serialize the data
    normalized = json.dumps(data, sort_keys=True, default=str)
    return hashlib.md5(normalized.encode()).hexdigest()


def is_recently_synced(state, record_id, source):
    """Check if a record was recently synced FROM the given source (to prevent loops)"""
    key = f"{source}:{record_id}"
    recent = state.get("recent_syncs", {})
    if key in recent:
        last_sync = datetime.fromisoformat(recent[key])
        if datetime.utcnow() - last_sync < timedelta(seconds=SYNC_COOLDOWN):
            return True
    return False


def mark_synced(state, record_id, source):
    """Mark a record as recently synced from a source"""
    if "recent_syncs" not in state:
        state["recent_syncs"] = {}
    key = f"{source}:{record_id}"
    state["recent_syncs"][key] = datetime.utcnow().isoformat()

    # Clean up old entries (older than 1 hour)
    cutoff = datetime.utcnow() - timedelta(hours=1)
    state["recent_syncs"] = {
        k: v for k, v in state["recent_syncs"].items()
        if datetime.fromisoformat(v) > cutoff
    }


# ==================== EZEKIA → FOLK WEBHOOKS ====================

@app.route('/webhook/person/new', methods=['POST'])
def webhook_person_new():
    """Handle new person from Ezekia → create in Folk"""
    data = request.json
    print(f"[Webhook] New person: {data}")

    # Transform Ezekia data to Folk format
    folk_data = {
        "firstName": data.get("first_name", ""),
        "lastName": data.get("last_name", ""),
        "jobTitle": data.get("job_title", ""),
    }

    # Handle arrays for email, phone, urls
    if data.get("email"):
        folk_data["emails"] = [{"value": data["email"]}]
    if data.get("phone"):
        folk_data["phones"] = [{"value": data["phone"]}]
    if data.get("linkedin_url"):
        folk_data["urls"] = [{"value": data["linkedin_url"]}]

    response = folk_client.create_person(folk_data)

    if response.status_code in [200, 201]:
        result = response.json()
        folk_id = result.get("id", "")

        # Mark as synced from Ezekia to prevent loop
        state = load_state()
        mark_synced(state, folk_id, "ezekia")
        save_state(state)

        return jsonify({"status": "success", "folk_id": folk_id})

    return jsonify({"status": "error", "message": response.text}), 500


@app.route('/webhook/person/update', methods=['POST'])
def webhook_person_update():
    """Handle person update from Ezekia → update in Folk"""
    data = request.json
    print(f"[Webhook] Update person: {data}")

    # Find existing person by email
    email = data.get("email", "")
    existing = folk_client.search_person_by_email(email) if email else None

    # Transform Ezekia data to Folk format
    folk_data = {
        "firstName": data.get("first_name", ""),
        "lastName": data.get("last_name", ""),
        "jobTitle": data.get("job_title", ""),
    }

    if data.get("email"):
        folk_data["emails"] = [{"value": data["email"]}]
    if data.get("phone"):
        folk_data["phones"] = [{"value": data["phone"]}]
    if data.get("linkedin_url"):
        folk_data["urls"] = [{"value": data["linkedin_url"]}]

    if existing:
        response = folk_client.update_person(existing["id"], folk_data)
        folk_id = existing["id"]
    else:
        response = folk_client.create_person(folk_data)
        folk_id = response.json().get("id", "") if response.status_code in [200, 201] else ""

    if response.status_code in [200, 201]:
        # Mark as synced from Ezekia
        state = load_state()
        mark_synced(state, folk_id, "ezekia")
        save_state(state)
        return jsonify({"status": "success", "folk_id": folk_id})

    return jsonify({"status": "error", "message": response.text}), 500


@app.route('/webhook/company/new', methods=['POST'])
def webhook_company_new():
    """Handle new company from Ezekia → create in Folk"""
    data = request.json
    print(f"[Webhook] New company: {data}")

    folk_data = {
        "name": data.get("name", data.get("company_name", "Unknown"))
    }

    if data.get("website"):
        folk_data["urls"] = [{"value": data["website"]}]

    response = folk_client.create_company(folk_data)

    if response.status_code in [200, 201]:
        result = response.json()
        folk_id = result.get("id", "")

        state = load_state()
        mark_synced(state, folk_id, "ezekia")
        save_state(state)

        return jsonify({"status": "success", "folk_id": folk_id})

    return jsonify({"status": "error", "message": response.text}), 500


@app.route('/webhook/company/update', methods=['POST'])
def webhook_company_update():
    """Handle company update from Ezekia → update in Folk"""
    data = request.json
    print(f"[Webhook] Update company: {data}")

    # For now, just acknowledge - company update needs ID lookup
    return jsonify({"status": "received"})


# ==================== EZEKIA NOTES → FOLK CUSTOM FIELD ====================

# Custom field name for Ezekia notes in Folk
EZEKIA_NOTES_FIELD = "Ezekia Notes"

@app.route('/webhook/note', methods=['POST'])
def webhook_note():
    """
    Handle notes from Ezekia → append to Folk custom field

    Expected payload:
    {
        "email": "person@email.com",  # Primary lookup
        "first_name": "John",          # Fallback lookup
        "last_name": "Doe",            # Fallback lookup
        "folk_id": "per_xxx",          # Direct lookup if available
        "note": "The actual note content",
        "note_type": "activity",       # Optional: activity, comment, etc.
        "note_date": "2024-01-15"      # Optional: date of the note
    }
    """
    data = request.json
    print(f"[Webhook] Ezekia note received: {data}")

    note_content = data.get("note", "").strip()
    if not note_content:
        return jsonify({"status": "error", "message": "No note content provided"}), 400

    # Find the person in Folk
    person = None
    folk_id = data.get("folk_id")

    if folk_id:
        # Direct lookup by Folk ID
        person = folk_client.get_person(folk_id)

    if not person:
        # Try email lookup
        email = data.get("email", "")
        if email:
            person = folk_client.search_person_by_email(email)

    if not person:
        # Try name lookup as fallback
        first_name = data.get("first_name", "")
        last_name = data.get("last_name", "")
        if first_name or last_name:
            person = folk_client.search_person_by_name(first_name, last_name)

    if not person:
        return jsonify({
            "status": "error",
            "message": "Person not found in Folk",
            "searched": {
                "folk_id": data.get("folk_id"),
                "email": data.get("email"),
                "name": f"{data.get('first_name', '')} {data.get('last_name', '')}"
            }
        }), 404

    person_id = person.get("id")
    person_name = f"{person.get('firstName', '')} {person.get('lastName', '')}".strip()

    # Get group ID for custom field
    group_id = folk_client.get_first_group_id()
    if not group_id:
        return jsonify({"status": "error", "message": "No Folk group found for custom fields"}), 500

    # Build note with metadata
    note_type = data.get("note_type", "note")
    note_date = data.get("note_date", "")

    formatted_note = f"[{note_type.upper()}]"
    if note_date:
        formatted_note += f" ({note_date})"
    formatted_note += f" {note_content}"

    # Append to custom field
    response = folk_client.append_to_custom_field(
        person_id,
        group_id,
        EZEKIA_NOTES_FIELD,
        formatted_note
    )

    if response and response.status_code in [200, 201]:
        # Mark as synced from Ezekia
        state = load_state()
        mark_synced(state, person_id, "ezekia")
        save_state(state)

        return jsonify({
            "status": "success",
            "folk_id": person_id,
            "person_name": person_name,
            "field": EZEKIA_NOTES_FIELD,
            "group_id": group_id
        })

    error_msg = response.text if response else "Unknown error"
    return jsonify({"status": "error", "message": error_msg}), 500


@app.route('/debug/groups', methods=['GET'])
def debug_groups():
    """Debug: List groups from Folk (needed for custom field setup)"""
    groups = folk_client.list_groups()
    return jsonify(groups)


# ==================== FOLK → EZEKIA SYNC ====================

def send_to_zapier(url, data):
    """Send data to Zapier webhook"""
    if not url:
        print(f"[Zapier] No webhook URL configured")
        return False

    try:
        response = requests.post(url, json=data, timeout=30)
        print(f"[Zapier] Sent to {url[:50]}... - Status: {response.status_code}")
        return response.status_code in [200, 201]
    except Exception as e:
        print(f"[Zapier] Error sending webhook: {e}")
        return False


def sync_folk_people_to_ezekia():
    """Sync people from Folk → Ezekia via Zapier"""
    state = load_state()
    people = folk_client.get_all_people()

    if not people:
        print("[Sync] No people found in Folk")
        return {"synced": 0, "skipped": 0, "errors": 0}

    print(f"[Sync] Processing {len(people)} people, first item type: {type(people[0]) if people else 'N/A'}")

    stats = {"synced": 0, "skipped": 0, "errors": 0}

    for person in people:
        # Handle case where person might be a list or other structure
        if isinstance(person, list) and len(person) > 0:
            person = person[0] if isinstance(person[0], dict) else {}
        if not isinstance(person, dict):
            print(f"[Sync] Skipping invalid person data type: {type(person)}")
            stats["errors"] += 1
            continue

        folk_id = person.get("id", "")

        # Skip if recently synced from Ezekia (prevent loop)
        if is_recently_synced(state, folk_id, "ezekia"):
            print(f"[Sync] Skipping {folk_id} - recently synced from Ezekia")
            stats["skipped"] += 1
            continue

        # Extract relevant fields for hash
        hash_data = {
            "firstName": person.get("firstName", ""),
            "lastName": person.get("lastName", ""),
            "emails": person.get("emails", []),
            "phones": person.get("phones", []),
            "jobTitle": person.get("jobTitle", ""),
        }
        current_hash = compute_hash(hash_data)

        # Check if this is new or changed
        stored = state["people"].get(folk_id, {})
        stored_hash = stored.get("hash", "")

        if current_hash == stored_hash:
            # No changes
            stats["skipped"] += 1
            continue

        # Transform Folk data to Ezekia format
        # Note: Folk API returns emails/phones/urls as arrays of strings, not objects
        emails = person.get("emails", [])
        phones = person.get("phones", [])
        urls = person.get("urls", [])

        ezekia_data = {
            "folk_id": folk_id,
            "first_name": person.get("firstName", ""),
            "last_name": person.get("lastName", ""),
            "email": emails[0] if emails and isinstance(emails[0], str) else "",
            "phone": phones[0] if phones and isinstance(phones[0], str) else "",
            "position_title": person.get("jobTitle", ""),
            "description": person.get("description", ""),  # Notes/bio from Folk
        }

        # Extract LinkedIn URL if present
        for url in urls:
            if isinstance(url, str) and "linkedin" in url.lower():
                ezekia_data["linkedin_url"] = url
                break

        # Extract company name if linked
        companies = person.get("companies", [])
        if companies and isinstance(companies[0], dict):
            ezekia_data["company_name"] = companies[0].get("name", "")

        # Determine if new or update
        is_new = folk_id not in state["people"]
        webhook_url = ZAPIER_PERSON_NEW_URL if is_new else ZAPIER_PERSON_UPDATE_URL

        if send_to_zapier(webhook_url, ezekia_data):
            # Update state
            state["people"][folk_id] = {
                "hash": current_hash,
                "last_synced": datetime.utcnow().isoformat()
            }
            mark_synced(state, folk_id, "folk")
            stats["synced"] += 1
        else:
            stats["errors"] += 1

    state["last_poll"] = datetime.utcnow().isoformat()
    save_state(state)

    return stats


def sync_folk_companies_to_ezekia():
    """Sync companies from Folk → Ezekia via Zapier"""
    state = load_state()
    companies = folk_client.get_all_companies()

    if not companies:
        print("[Sync] No companies found in Folk")
        return {"synced": 0, "skipped": 0, "errors": 0}

    stats = {"synced": 0, "skipped": 0, "errors": 0}

    for company in companies:
        # Handle case where company might be a list or other structure
        if isinstance(company, list) and len(company) > 0:
            company = company[0] if isinstance(company[0], dict) else {}
        if not isinstance(company, dict):
            print(f"[Sync] Skipping invalid company data type: {type(company)}")
            stats["errors"] += 1
            continue

        folk_id = company.get("id", "")

        # Skip if recently synced from Ezekia
        if is_recently_synced(state, folk_id, "ezekia"):
            print(f"[Sync] Skipping company {folk_id} - recently synced from Ezekia")
            stats["skipped"] += 1
            continue

        # Extract relevant fields for hash
        hash_data = {
            "name": company.get("name", ""),
            "urls": company.get("urls", []),
        }
        current_hash = compute_hash(hash_data)

        # Check if new or changed
        stored = state["companies"].get(folk_id, {})
        stored_hash = stored.get("hash", "")

        if current_hash == stored_hash:
            stats["skipped"] += 1
            continue

        # Transform to Ezekia format
        # Note: Folk API returns urls as arrays of strings, not objects
        urls = company.get("urls", [])
        ezekia_data = {
            "folk_id": folk_id,
            "name": company.get("name", ""),
            "website": urls[0] if urls and isinstance(urls[0], str) else "",
        }

        # Determine if new or update
        is_new = folk_id not in state["companies"]
        webhook_url = ZAPIER_COMPANY_NEW_URL if is_new else ZAPIER_COMPANY_UPDATE_URL

        if send_to_zapier(webhook_url, ezekia_data):
            state["companies"][folk_id] = {
                "hash": current_hash,
                "last_synced": datetime.utcnow().isoformat()
            }
            mark_synced(state, folk_id, "folk")
            stats["synced"] += 1
        else:
            stats["errors"] += 1

    state["last_poll"] = datetime.utcnow().isoformat()
    save_state(state)

    return stats


@app.route('/sync/folk-to-ezekia', methods=['POST'])
def sync_folk_to_ezekia():
    """Trigger Folk → Ezekia sync (called by cron or manually)"""
    print(f"[Sync] Starting Folk → Ezekia sync at {datetime.utcnow()}")

    people_stats = sync_folk_people_to_ezekia()
    company_stats = sync_folk_companies_to_ezekia()

    result = {
        "status": "completed",
        "timestamp": datetime.utcnow().isoformat(),
        "people": people_stats,
        "companies": company_stats
    }

    print(f"[Sync] Completed: {result}")
    return jsonify(result)


# ==================== DEBUG ENDPOINTS ====================

@app.route('/debug/folk-people', methods=['GET'])
def debug_folk_people():
    """Debug: List people from Folk"""
    people = folk_client.get_all_people()
    return jsonify({"count": len(people) if people else 0, "data": people})


@app.route('/debug/folk-companies', methods=['GET'])
def debug_folk_companies():
    """Debug: List companies from Folk"""
    companies = folk_client.get_all_companies()
    return jsonify({"count": len(companies) if companies else 0, "data": companies})


@app.route('/debug/state', methods=['GET'])
def debug_state():
    """Debug: View current sync state"""
    state = load_state()
    return jsonify(state)


@app.route('/debug/reset-state', methods=['POST'])
def debug_reset_state():
    """Debug: Reset sync state (use with caution)"""
    save_state({
        "last_poll": None,
        "people": {},
        "companies": {},
        "recent_syncs": {}
    })
    return jsonify({"status": "state reset"})


# ==================== HEALTH CHECK ====================

@app.route('/', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "folk-ezekia-sync",
        "version": "2.1.0",
        "features": ["ezekia-to-folk", "folk-to-ezekia", "ezekia-notes-to-folk"],
        "folk_connected": folk_client.test_connection() if FOLK_API_KEY else False,
        "zapier_configured": {
            "person_new": bool(ZAPIER_PERSON_NEW_URL),
            "person_update": bool(ZAPIER_PERSON_UPDATE_URL),
            "company_new": bool(ZAPIER_COMPANY_NEW_URL),
            "company_update": bool(ZAPIER_COMPANY_UPDATE_URL),
        },
        "notes_field": EZEKIA_NOTES_FIELD
    })


if __name__ == '__main__':
    print(f"Starting Folk-Ezekia Sync Server on port {PORT}")
    print(f"Folk API Key configured: {bool(FOLK_API_KEY)}")
    print(f"Zapier webhooks configured: person_new={bool(ZAPIER_PERSON_NEW_URL)}, person_update={bool(ZAPIER_PERSON_UPDATE_URL)}")
    app.run(host='0.0.0.0', port=PORT, debug=False)
