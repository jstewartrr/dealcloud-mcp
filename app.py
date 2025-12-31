"""
DealCloud MCP Server v2.0 - Fixed API Endpoints
Provides full CRUD access to DealCloud via REST API v4.
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
    """Make API request to DealCloud."""
    token = get_access_token()
    base = f"{DEALCLOUD_URL}/api/rest/{api_version}"
    url = f"{base}{endpoint}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    logger.info(f"API Request: {method} {url}")
    
    try:
        if method == "GET":
            response = requests.get(url, headers=headers, timeout=60)
        elif method == "POST":
            response = requests.post(url, headers=headers, json=data, timeout=60)
        elif method == "PATCH":
            response = requests.patch(url, headers=headers, json=data, timeout=60)
        elif method == "PUT":
            response = requests.put(url, headers=headers, json=data, timeout=60)
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
            detail = response.text[:500] if response.text else str(e)
        return {"error": str(e), "detail": detail}
    except Exception as e:
        return {"error": str(e)}

# =============================================================================
# TOOL IMPLEMENTATIONS
# =============================================================================

def tool_test_connection():
    """Test DealCloud API connection."""
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
    """List all entry types."""
    return api_request("GET", "/schema/entryTypes")

def tool_get_entry_type(entry_type_id):
    """Get details of a specific entry type."""
    return api_request("GET", f"/schema/entryTypes/{entry_type_id}")

def tool_get_fields(entry_type_id):
    """Get all fields for an entry type."""
    return api_request("GET", f"/schema/entryTypes/{entry_type_id}/fields")

def tool_get_field(entry_type_id, field_id):
    """Get details of a specific field."""
    return api_request("GET", f"/schema/entryTypes/{entry_type_id}/fields/{field_id}")

def tool_search_entries(entry_type_id, query=None, fields=None, limit=100, skip=0):
    """
    Search/query entries using the correct v4 API endpoint.
    POST /data/entrydata/rows/query/{entryTypeId}
    """
    payload = {
        "limit": limit,
        "skip": skip,
        "resolveReferenceUrls": True,
        "wrapIntoArrays": True
    }
    
    if query:
        # Query can be a string like "{CompanyName: \"Test\"}" or a dict
        if isinstance(query, dict):
            payload["query"] = json.dumps(query)
        else:
            payload["query"] = query
    
    if fields:
        payload["fields"] = fields
    
    return api_request("POST", f"/data/entrydata/rows/query/{entry_type_id}", data=payload)

def tool_get_entry(entry_type_id, entry_id):
    """Get a specific entry by ID using query."""
    payload = {
        "query": f"{{entryid: {entry_id}}}",
        "limit": 1,
        "resolveReferenceUrls": True,
        "wrapIntoArrays": True
    }
    result = api_request("POST", f"/data/entrydata/rows/query/{entry_type_id}", data=payload)
    
    # Return first entry if found
    if isinstance(result, list) and len(result) > 0:
        return result[0]
    return result

def tool_create_entry(entry_type_id, data):
    """
    Create a new entry using the correct v4 API endpoint.
    POST /data/entrydata/rows/{entryTypeId}
    """
    # Data should be an array of entries
    entries = data if isinstance(data, list) else [data]
    return api_request("POST", f"/data/entrydata/rows/{entry_type_id}", data=entries)

def tool_update_entry(entry_type_id, entry_id, data):
    """
    Update an existing entry.
    PUT /data/entrydata/rows/{entryTypeId}
    """
    # Ensure EntryId is included in the data
    entry_data = data.copy() if isinstance(data, dict) else data
    if isinstance(entry_data, dict):
        entry_data["EntryId"] = int(entry_id)
        entries = [entry_data]
    else:
        entries = entry_data
    
    return api_request("PUT", f"/data/entrydata/rows/{entry_type_id}", data=entries)

def tool_delete_entry(entry_type_id, entry_id):
    """
    Delete an entry.
    DELETE /data/entrydata/{entryTypeId}/entries/{entryId}
    """
    return api_request("DELETE", f"/data/entrydata/{entry_type_id}/entries/{entry_id}")

def tool_get_choice_values(field_id):
    """Get dropdown/choice values for a field."""
    return api_request("GET", f"/schema/fields/{field_id}/choices")

def tool_get_relationships(entry_type_id, entry_id):
    """Get relationships for an entry."""
    # Use the query endpoint to get entry with relationships
    payload = {
        "query": f"{{entryid: {entry_id}}}",
        "limit": 1,
        "resolveReferenceUrls": True,
        "wrapIntoArrays": True
    }
    return api_request("POST", f"/data/entrydata/rows/query/{entry_type_id}", data=payload)

def tool_get_history(entry_type_id, modified_since):
    """Get modified entries since a given datetime."""
    return api_request("GET", f"/data/entrydata/{entry_type_id}/entries/history?modifiedSince={modified_since}")

# =============================================================================
# TOOL DEFINITIONS
# =============================================================================

TOOLS = [
    {"name": "test_connection", "description": "Test DealCloud API connection", "inputSchema": {"type": "object", "properties": {}, "required": []}},
    {"name": "list_entry_types", "description": "List all entry types (Companies, Deals, Contacts, etc.)", "inputSchema": {"type": "object", "properties": {}, "required": []}},
    {"name": "get_entry_type", "description": "Get details of a specific entry type by ID or apiName", "inputSchema": {"type": "object", "properties": {"entry_type_id": {"type": "string", "description": "Entry type ID or apiName (e.g., '11919' or 'Deal')"}}, "required": ["entry_type_id"]}},
    {"name": "get_fields", "description": "Get all fields for an entry type", "inputSchema": {"type": "object", "properties": {"entry_type_id": {"type": "string", "description": "Entry type ID or apiName"}}, "required": ["entry_type_id"]}},
    {"name": "get_field", "description": "Get details of a specific field", "inputSchema": {"type": "object", "properties": {"entry_type_id": {"type": "string"}, "field_id": {"type": "string"}}, "required": ["entry_type_id", "field_id"]}},
    {"name": "search_entries", "description": "Search/query entries in an entry type. Query format: {fieldApiName: \"value\"}", "inputSchema": {"type": "object", "properties": {"entry_type_id": {"type": "string", "description": "Entry type ID or apiName"}, "query": {"type": "string", "description": "MongoDB-style query string, e.g., {CompanyName: \"Test\"}"}, "fields": {"type": "array", "items": {"type": "string"}, "description": "List of field apiNames to return"}, "limit": {"type": "integer", "default": 100}, "skip": {"type": "integer", "default": 0}}, "required": ["entry_type_id"]}},
    {"name": "get_entry", "description": "Get a specific entry by ID", "inputSchema": {"type": "object", "properties": {"entry_type_id": {"type": "string"}, "entry_id": {"type": "string"}}, "required": ["entry_type_id", "entry_id"]}},
    {"name": "create_entry", "description": "Create a new entry. Data should be object with field apiNames as keys", "inputSchema": {"type": "object", "properties": {"entry_type_id": {"type": "string"}, "data": {"type": "object", "description": "Entry data with field apiNames as keys"}}, "required": ["entry_type_id", "data"]}},
    {"name": "update_entry", "description": "Update an existing entry", "inputSchema": {"type": "object", "properties": {"entry_type_id": {"type": "string"}, "entry_id": {"type": "string"}, "data": {"type": "object", "description": "Fields to update"}}, "required": ["entry_type_id", "entry_id", "data"]}},
    {"name": "delete_entry", "description": "Delete an entry", "inputSchema": {"type": "object", "properties": {"entry_type_id": {"type": "string"}, "entry_id": {"type": "string"}}, "required": ["entry_type_id", "entry_id"]}},
    {"name": "get_choice_values", "description": "Get dropdown values for a choice field", "inputSchema": {"type": "object", "properties": {"field_id": {"type": "string"}}, "required": ["field_id"]}},
    {"name": "get_relationships", "description": "Get all data for an entry including relationships", "inputSchema": {"type": "object", "properties": {"entry_type_id": {"type": "string"}, "entry_id": {"type": "string"}}, "required": ["entry_type_id", "entry_id"]}},
    {"name": "get_history", "description": "Get entries modified since a datetime (ISO format)", "inputSchema": {"type": "object", "properties": {"entry_type_id": {"type": "string"}, "modified_since": {"type": "string", "description": "ISO datetime, e.g., 2024-01-01T00:00:00Z"}}, "required": ["entry_type_id", "modified_since"]}},
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
        "search_entries": lambda: tool_search_entries(
            args["entry_type_id"], 
            args.get("query"), 
            args.get("fields"),
            args.get("limit", 100),
            args.get("skip", 0)
        ),
        "get_entry": lambda: tool_get_entry(args["entry_type_id"], args["entry_id"]),
        "create_entry": lambda: tool_create_entry(args["entry_type_id"], args["data"]),
        "update_entry": lambda: tool_update_entry(args["entry_type_id"], args["entry_id"], args["data"]),
        "delete_entry": lambda: tool_delete_entry(args["entry_type_id"], args["entry_id"]),
        "get_choice_values": lambda: tool_get_choice_values(args["field_id"]),
        "get_relationships": lambda: tool_get_relationships(args["entry_type_id"], args["entry_id"]),
        "get_history": lambda: tool_get_history(args["entry_type_id"], args["modified_since"]),
    }
    
    if name not in tool_map:
        return {"content": [{"type": "text", "text": f"Unknown tool: {name}"}], "isError": True}
    
    try:
        result = tool_map[name]()
        return {"content": [{"type": "text", "text": json.dumps(result, indent=2, default=str)}]}
    except Exception as e:
        logger.error(f"Tool error: {e}")
        return {"content": [{"type": "text", "text": f"Error: {str(e)}"}], "isError": True}

def process_mcp(data):
    method = data.get("method", "")
    params = data.get("params", {})
    req_id = data.get("id", 1)
    
    if method == "initialize":
        return {"jsonrpc": "2.0", "id": req_id, "result": {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {"listChanged": True}},
            "serverInfo": {"name": "dealcloud-mcp", "version": "2.0.0"}
        }}
    elif method == "tools/list":
        return {"jsonrpc": "2.0", "id": req_id, "result": {"tools": TOOLS}}
    elif method == "tools/call":
        result = handle_tools_call(params)
        return {"jsonrpc": "2.0", "id": req_id, "result": result}
    elif method == "notifications/initialized":
        return None
    else:
        return {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": f"Method not found: {method}"}}

# =============================================================================
# SSE Support
# =============================================================================

sse_sessions = {}

@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "healthy",
        "service": "dealcloud-mcp",
        "version": "2.0.0",
        "dealcloud_url": DEALCLOUD_URL
    })

@app.route("/mcp", methods=["POST"])
def mcp_handler():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}}), 400
        response = process_mcp(data)
        if response is None:
            return "", 204
        return jsonify(response)
    except Exception as e:
        logger.error(f"MCP handler error: {e}")
        return jsonify({"jsonrpc": "2.0", "id": 1, "error": {"code": -32603, "message": str(e)}}), 500

@app.route("/sse", methods=["GET"])
def sse_connect():
    session_id = str(uuid.uuid4())
    sse_sessions[session_id] = queue.Queue()
    
    def generate():
        yield f"event: endpoint\ndata: /sse/{session_id}/message\n\n"
        while True:
            try:
                message = sse_sessions[session_id].get(timeout=30)
                yield f"event: message\ndata: {json.dumps(message)}\n\n"
            except queue.Empty:
                yield ": keepalive\n\n"
            except GeneratorExit:
                break
        if session_id in sse_sessions:
            del sse_sessions[session_id]
    
    return Response(generate(), mimetype="text/event-stream", headers={
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no"
    })

@app.route("/sse/<session_id>/message", methods=["POST"])
def sse_message(session_id):
    if session_id not in sse_sessions:
        return jsonify({"error": "Session not found"}), 404
    
    data = request.get_json()
    response = process_mcp(data)
    if response:
        sse_sessions[session_id].put(response)
    return "", 202

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
