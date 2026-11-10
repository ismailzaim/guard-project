
# Databricks notebook source

# ===============================
# 1. Define Connection Variables
# ===============================

import requests
import json

secret_scope_name = "guard-scope"

# Credentials placeholders
# Replace these values only in your private Databricks environment.
kafka_bootstrap_servers = "<YOUR_KAFKA_BOOTSTRAP_SERVERS>"
kafka_topic = "credit_card_transactions"
kafka_api_key = "<YOUR_KAFKA_API_KEY>"
kafka_api_secret = "<YOUR_KAFKA_API_SECRET>"

gmail_api_key = "<YOUR_GMAIL_APP_PASSWORD>"


# COMMAND ----------

# ===============================
# 2. Retrieve Databricks Context
# ===============================

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

api_url = ctx.apiUrl().getOrElse(None)
api_token = ctx.apiToken().getOrElse(None)

# Never print api_token or other credentials.

if not api_url or not api_token:
    raise ValueError("Databricks workspace URL or token is unavailable.")

DATABRICKS_INSTANCE = api_url
DATABRICKS_TOKEN = api_token


# COMMAND ----------

# ===============================
# 3. Create Secret Scope
# ===============================

url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/scopes/create"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "scope": secret_scope_name,
    "initial_manage_principal": "users"
}

response = requests.post(url, headers=headers, json=payload)

if response.status_code == 200:
    print(f"Secret scope '{secret_scope_name}' created successfully.")
elif response.status_code == 409:
    print(f"Secret scope '{secret_scope_name}' already exists.")
else:
    print("Failed to create secret scope.")
    print("Status Code:", response.status_code)
    print("Response:", response.text)


# COMMAND ----------

# ===============================
# 4. Prepare Kafka Connection Details
# ===============================

kafka_connection_details = json.dumps({
    "bootstrap_servers": kafka_bootstrap_servers,
    "topic": kafka_topic,
    "api_key": kafka_api_key,
    "api_secret": kafka_api_secret
})


# COMMAND ----------

# ===============================
# 5. Store Kafka Credentials
# ===============================

def store_secret(scope, key, value):

    url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/put"

    payload = {
        "scope": scope,
        "key": key,
        "string_value": value
    }

    response = requests.post(
        url,
        headers=headers,
        json=payload
    )

    if response.status_code == 200:
        print(f"Secret '{key}' stored successfully.")
    else:
        raise RuntimeError(
            f"Failed to store secret '{key}'. "
            f"Status: {response.status_code}"
        )


# Do not store placeholder values.
if (
    "<YOUR_KAFKA_API_KEY>" not in kafka_api_key
    and "<YOUR_KAFKA_API_SECRET>" not in kafka_api_secret
    and "<YOUR_KAFKA_BOOTSTRAP_SERVERS>" not in kafka_bootstrap_servers
):
    store_secret(
        secret_scope_name,
        "kafka_connection_details",
        kafka_connection_details
    )
else:
    print("Kafka credentials are placeholders. Secret not stored.")


# COMMAND ----------

# ===============================
# 6. Store Gmail Credentials
# ===============================

if "<YOUR_GMAIL_APP_PASSWORD>" not in gmail_api_key:
    store_secret(
        secret_scope_name,
        "gmail_api_key",
        gmail_api_key
    )
else:
    print("Gmail password is a placeholder. Secret not stored.")


# COMMAND ----------

# ===============================
# 7. Verify Secret Availability
# ===============================

try:
    retrieved_json = dbutils.secrets.get(
        scope=secret_scope_name,
        key="kafka_connection_details"
    )

    parsed = json.loads(retrieved_json)

    print("Kafka connection secret retrieved successfully.")
    print("Kafka secret structure verified.")

except Exception:
    print("Kafka secret verification failed.")
