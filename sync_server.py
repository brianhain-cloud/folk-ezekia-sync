#!/usr/bin/env python3
"""
Folk-Ezekia Sync Server - Cloud Ready
"""

import os
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

# Configuration from environment variables
FOLK_API_KEY = os.environ.get('FOLK_API_KEY', '')
FOLK_BASE_URL = "https://api.folk.app/v1"
PORT = int(os.environ.get('PORT', 5001))


class FolkClient:
    def __init__(self):
        self.api_key = FOLK_API_KEY
        self.base_url = FOLK_BASE_URL
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def test_connection(self) -> bool:
        try:
            response = requests.get(f"{self.base_url}/groups", headers=self.headers)
            return response.status_code == 200
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False

    def create_person(self, data: dict, group_id: str = None) -> dict:
        person_data = {}
        if "firstName" in data:
            person_data["firstName"] = data["firstName"]
        if "lastName" in data:
            person_data["lastName"] = data["lastName"]
        if "jobTitle" in data:
            person_data["jobTitle"] = data["jobTitle"]
        if "emails" in data and data["emails"]:
            person_data["emails"] = data["emails"] if isinstance(data["emails"], list) else [data["emails"]]
        if "phones" in data and data["phones"]:
            person_data["phones"] = data["phones"] if isinstance(data["phones"], list) else [data["phones"]]
        if "urls" in data and data["urls"]:
            person_data["urls"] = data["urls"] if isinstance(data["urls"], list) else [data["urls"]]
        if group_id:
            person_data["groupIds"] = [group_id]

        print(f"[Folk API] Creating person: {person_data}")
        response = requests.post(f"{self.base_url}/people", headers=self.headers, json=person_data)
        print(f"[Folk API] Response: {response.status_code} - {response.text}")

        if response.status_code not in [200, 201]:
            raise Exception(f"Folk API error {response.status_code}: {response.text}")
        return response.json()

    def update_person(self, person_id: str, data: dict) -> dict:
        response = requests.patch(f"{self.base_url}/people/{person_id}", headers=self.headers, json=data)
        return response.json()

    def search_person_by_email(self, email: str):
        response = requests.get(f"{self.base_url}/people", headers=self.headers, params={"limit": 100})
        if response.status_code != 200:
            return None
        data = response.json()
        people = data.get("data", {}).get("items", [])
        for person in people:
            person_emails = person.get("emails", [])
            for e in person_emails:
                if e and email.lower() == e.lower():
                    return person
        return None

    def create_company(self, data: dict, group_id: str = None) -> dict:
        company_data = {"name": data.get("name", "Unknown Company")}
        if "website" in data:
            company_data["urls"] = [data["website"]]
        if group_id:
            company_data["groupIds"] = [group_id]

        response = requests.post(f"{self.base_url}/companies", headers=self.headers, json=company_data)
        if response.status_code not in [200, 201]:
            raise Exception(f"Folk API error {response.status_code}: {response.text}")
        return response.json()


folk = FolkClient()


@app.route('/', methods=['GET'])
def home():
    return jsonify({"status": "Folk-Ezekia Sync Server is running"})


@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "ok", "folk_connected": folk.test_connection()})


@app.route('/webhook/person/new', methods=['POST'])
def handle_new_person():
    try:
        data = request.json
        print(f"[Webhook] New person: {data}")

        folk_data = {}
        if data.get("first_name"):
            folk_data["firstName"] = data["first_name"]
        if data.get("last_name"):
            folk_data["lastName"] = data["last_name"]
        if data.get("job_title"):
            folk_data["jobTitle"] = data["job_title"]
        if data.get("email"):
            folk_data["emails"] = [data["email"]]
        if data.get("phone"):
            folk_data["phones"] = [data["phone"]]
        if data.get("linkedin_url"):
            folk_data["urls"] = [data["linkedin_url"]]

        result = folk.create_person(folk_data)
        return jsonify({"status": "success", "folk_id": result.get('id')}), 201
    except Exception as e:
        print(f"[Webhook] Error: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/webhook/person/update', methods=['POST'])
def handle_update_person():
    try:
        data = request.json
        print(f"[Webhook] Update person: {data}")

        email = data.get('email')
        if email:
            existing = folk.search_person_by_email(email)
            if existing:
                folk_data = {}
                if data.get("first_name"):
                    folk_data["firstName"] = data["first_name"]
                if data.get("last_name"):
                    folk_data["lastName"] = data["last_name"]
                if data.get("job_title"):
                    folk_data["jobTitle"] = data["job_title"]
                if data.get("phone"):
                    folk_data["phones"] = [data["phone"]]
                if data.get("linkedin_url"):
                    folk_data["urls"] = [data["linkedin_url"]]

                result = folk.update_person(existing['id'], folk_data)
                return jsonify({"status": "updated", "folk_id": existing['id']}), 200

        # If no existing person found, create new
        return handle_new_person()
    except Exception as e:
        print(f"[Webhook] Error: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/webhook/company/new', methods=['POST'])
def handle_new_company():
    try:
        data = request.json
        print(f"[Webhook] New company: {data}")

        folk_data = {"name": data.get("name", data.get("company_name", "Unknown"))}
        if data.get("website"):
            folk_data["website"] = data["website"]

        result = folk.create_company(folk_data)
        return jsonify({"status": "success", "folk_id": result.get('id')}), 201
    except Exception as e:
        print(f"[Webhook] Error: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/webhook/company/update', methods=['POST'])
def handle_update_company():
    try:
        data = request.json
        print(f"[Webhook] Update company: {data}")
        return jsonify({"status": "received"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == '__main__':
    print("=" * 60)
    print("FOLK-EZEKIA SYNC SERVER")
    print("=" * 60)
    print(f"Starting on port {PORT}...")
    app.run(host='0.0.0.0', port=PORT, debug=False)
