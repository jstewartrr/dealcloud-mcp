"""
DealCloud MCP Server - Production Build
Provides full CRUD access to DealCloud via REST API.
"""

import os
import json
import requests
import uuid
import queue
from flask import Flask, request, jsonify, Response
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# =============================================================================
# DEALCLOUD CONFIGURATION
# =============================================================================

DEALCLOUD_URL = os.environ.get("DEALCLOUD_URL", "https://middleground.dealcloud.com")
CLIENT_ID = os.environ.get("DEALCLOUD_CLIENT_ID", "6983")
CLIENT_SECRET = os.environ.get("DEALCLOUD_CLIENT_SECRET", "")

_token_cache = {"access_token": None, "expires_at": None}

# =============================================================================
# AUTHENTICATION
# =============================================================================

def get_access_token():
    global _token_cache
    
    if _token_cache["access_token"] and _token_cache["expires_at"]:
        if datetime.now() < _token_cache["expires_at"] - timedelta(seconds=60):
            return _token_cache["access_token"]
    
    token_url = f"{DEALCLOUD_URL}/api/rest/v1/oauth/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "data"
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    
    try:
        response = requests.post(token_url, data=data, headers=headers, timeout=30)
        response.raise_for_status()
        token_data = response.json()
        _token_cache["access_token"] = token_data["access_token"]
        _token_cache["expires_at"] = datetime.now() + timedelta(seconds=token_data.get("expires_in", 900))
        logger.info("Successfully obtained DealCloud access token")
        return _token_cache["access_token"]
    except Exception as e:
        logger.error(f"Failed to get DealCloud token: {e}")
        raise

def api_request(method, endpoint, data=None, api_version="v4"):
    token = get_access_token()
    base = f"{DEALCLOUD_URL}/api/rest/{api_version}"
    url = f"{base}{endpoint}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    try:
        if method == "GET":
            response = requests.get(url, headers=headers, timeout=60)
        elif method == "POST":
            response = requests.post(url, headers=headers, json=data, timeout=60)
        elif method == "PATCH":
            response = requests.patch(url, headers=headers, json=data, timeout=60)
        elif method == "DELETE":
            response = requests.delete(url, headers=headers, timeout=60)
        else:
            return {"error": f"Unsupported method: {method}"}
        
        response.raise_for_status()
        return response.json() if response.text else {"success": True}
    except requests.exceptions.HTTPError as e:
        try:
            detail = response.json()
        except:
            detail = response.text
        return {"error": str(e), "detail": detail}
    except Exception as e:
        return {"error": str(e)}

# =============================================================================
# TOOL IMPLEMENTATIONS
# =============================================================================

def tool_test_connection():
    try:
        get_access_token()
        entry_types = api_request("GET", "/schema/entryTypes")
        return {
            "status": "connected",
            "dealcloud_url": DEALCLOUD_URL,
            "client_id": CLIENT_ID,
            "entry_types_count": len(entry_types) if isinstance(entry_types, list) else "unknown"
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}

def tool_list_entry_types():
    return api_request("GET", "/schema/entryTypes")

def tool_get_entry_type(entry_type_id):
    return api_request("GET", f"/schema/entryTypes/{entry_type_id}")

def tool_get_fields(entry_type_id):
    return api_request("GET", f"/schema/entryTypes/{entry_type_id}/fields")

def tool_get_field(entry_type_id, field_id):
    return api_request("GET", f"/schema/entryTypes/{entry_type_id}/fields/{field_id}")

def tool_search_entries(entry_type_id, query=None, limit=100):
    endpoint = f"/data/entryTypes/{entry_type_id}/entries?$top={limit}"
    if query:
        endpoint += f"&$filter={json.dumps(query)}"
    return api_request("GET", endpoint)

def tool_get_entry(entry_type_id, entry_id):
    return api_request("GET", f"/data/entryTypes/{entry_type_id}/entries/{entry_id}")

def tool_create_entry(entry_type_id, data):
    return api_request("POST", f"/data/entryTypes/{entry_type_id}/entries", data=data)

def tool_update_entry(entry_type_id, entry_id, data):
    return api_request("PATCH", f"/data/entryTypes/{entry_type_id}/entries/{entry_id}", data=data)

def tool_delete_entry(entry_type_id, entry_id):
    return api_request("DELETE", f"/data/entryTypes/{entry_type_id}/entries/{entry_id}")

def tool_get_choice_values(field_id):
    return api_request("GET", f"/schema/fields/{field_id}/choices")

def tool_get_relationships(entry_type_id, entry_id):
    return api_request("GET", f"/data/entryTypes/{entry_type_id}/entries/{entry_id}/relationships")

# =============================================================================
# TOOL DEFINITIONS
# =============================================================================

TOOLS = [
    {"name": "test_connection", "description": "Test DealCloud API connection", "inputSchema": {"type": "object", "properties": {}, "required": []}},
    {"name": "list_entry_types", "description": "List all entry types (Companies, Deals, Contacts, etc.)", "inputSchema": {"type": "object", "properties": {}, "required": []}},
    {"name": "get_entry_type", "description": "Get details of a specific entry type by ID", "inputSchema": {"type": "object", "properties": {"entry_type_id": {"type": "string", "description": "The entry type ID"}}, "required": ["entry_type_id"]}},
    {"name": "get_fields", "description": "Get all fields for an entry type", "inputSchema": {"type": "object", "properties": {"entry_type_id": {"type": "string", "description": "The entry type ID"}}, "required": ["entry_type_id"]}},
    {"name": "get_field", "description": "Get details of a specific field", "inputSchema": {"type": "object", "properties": {"entry_type_id": {"type": "string"}, "field_id": {"type": "string"}}, "required": ["entry_type_id", "field_id"]}},
    {"name": "search_entries", "description": "Search/filter entries in an entry type", "inputSchema": {"type": "object", "properties": {"entry_type_id": {"type": "string"}, "query": {"type": "object"}, "limit": {"type": "integer", "default": 100}}, "required": ["entry_type_id"]}},
    {"name": "get_entry", "description": "Get a specific entry by ID", "inputSchema": {"type": "object", "properties": {"entry_type_id": {"type": "string"}, "entry_id": {"type": "string"}}, "required": ["entry_type_id", "entry_id"]}},
    {"name": "create_entry", "description": "Create a new entry", "inputSchema": {"type": "object", "properties": {"entry_type_id": {"type": "string"}, "data": {"type": "object"}}, "required": ["entry_type_id", "data"]}},
    {"name": "update_entry", "description": "Update an existing entry", "inputSchema": {"type": "object", "properties": {"entry_type_id": {"type": "string"}, "entry_id": {"type": "string"}, "data": {"type": "object"}}, "required": ["entry_type_id", "entry_id", "data"]}},
    {"name": "delete_entry", "description": "Delete an entry", "inputSchema": {"type": "object", "properties": {"entry_type_id": {"type": "string"}, "entry_id": {"type": "string"}}, "required": ["entry_type_id", "entry_id"]}},
    {"name": "get_choice_values", "description": "Get dropdown values for a field", "inputSchema": {"type": "object", "properties": {"field_id": {"type": "string"}}, "required": ["field_id"]}},
    {"name": "get_relationships", "description": "Get relationships for an entry", "inputSchema": {"type": "object", "properties": {"entry_type_id": {"type": "string"}, "entry_id": {"type": "string"}}, "required": ["entry_type_id", "entry_id"]}},
]

# =============================================================================
# MCP HANDLERS
# =============================================================================

def handle_tools_call(params):
    name = params.get("name", "")
    args = params.get("arguments", {})
    
    tool_map = {
        "test_connection": lambda: tool_test_connection(),
        "list_entry_types": lambda: tool_list_entry_types(),
        "get_entry_type": lambda: tool_get_entry_type(args["entry_type_id"]),
        "get_fields": lambda: tool_get_fields(args["entry_type_id"]),
        "get_field": lambda: tool_get_field(args["entry_type_id"], args["field_id"]),
        "search_entries": lambda: tool_search_entries(args["entry_type_id"], args.get("query"), args.get("limit", 100)),
        "get_entry": lambda: tool_get_entry(args["entry_type_id"], args["entry_id"]),
        "create_entry": lambda: tool_create_entry(args["entry_type_id"], args["data"]),
        "update_entry": lambda: tool_update_entry(args["entry_type_id"], args["entry_id"], args["data"]),
        "delete_entry": lambda: tool_delete_entry(args["entry_type_id"], args["entry_id"]),
        "get_choice_values": lambda: tool_get_choice_values(args["field_id"]),
        "get_relationships": lambda: tool_get_relationships(args["entry_type_id"], args["entry_id"]),
    }
    
    if name not in tool_map:
        return {"content": [{"type": "text", "text": f"Unknown tool: {name}"}], "isError": True}
    
    try:
        result = tool_map[name]()
        return {"content": [{"type": "text", "text": json.dumps(result, indent=2, default=str)}]}
    except Exception as e:
        return {"content": [{"type": "text", "text": f"Error: {str(e)}"}], "isError": True}

def process_mcp(data):
    method = data.get("method", "")
    params = data.get("params", {})
    req_id = data.get("id", 1)
    
    if method == "initialize":
        result = {"protocolVersion": "2024-11-05", "capabilities": {"tools": {"listChanged": False}}, "serverInfo": {"name": "dealcloud-mcp", "version": "1.0.0"}}
    elif method == "tools/list":
        result = {"tools": TOOLS}
    elif method == "tools/call":
        result = handle_tools_call(params)
    elif method == "notifications/initialized":
        return {"jsonrpc": "2.0", "id": req_id, "result": {}}
    else:
        return {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": f"Unknown: {method}"}}
    
    return {"jsonrpc": "2.0", "id": req_id, "result": result}

# =============================================================================
# FLASK ROUTES
# =============================================================================

sse_sessions = {}

@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "service": "dealcloud-mcp", "version": "1.0.0", "tools": len(TOOLS)})

@app.route("/mcp", methods=["POST"])
def mcp_handler():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}}), 400
        return jsonify(process_mcp(data))
    except Exception as e:
        return jsonify({"jsonrpc": "2.0", "id": 1, "error": {"code": -32603, "message": str(e)}}), 500

@app.route("/sse", methods=["GET"])
def sse_connect():
    session_id = str(uuid.uuid4())
    sse_sessions[session_id] = queue.Queue()
    
    def generate():
        yield f"event: endpoint\ndata: /sse/{session_id}/message\n\n"
        while True:
            try:
                msg = sse_sessions[session_id].get(timeout=30)
                yield f"event: message\ndata: {json.dumps(msg)}\n\n"
            except queue.Empty:
                yield ": keepalive\n\n"
            except:
                break
        sse_sessions.pop(session_id, None)
    
    return Response(generate(), mimetype="text/event-stream", headers={"Cache-Control": "no-cache", "Connection": "keep-alive"})

@app.route("/sse/<session_id>/message", methods=["POST"])
def sse_message(session_id):
    if session_id not in sse_sessions:
        return jsonify({"error": "Session not found"}), 404
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data"}), 400
        sse_sessions[session_id].put(process_mcp(data))
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    logger.info("DealCloud MCP Server starting...")
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
