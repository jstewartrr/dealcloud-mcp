"""
DealCloud MCP Server for SM Gateway Integration
================================================
Provides full CRUD access to DealCloud via REST API.
Deployed as backend service to SM Gateway.
"""

import os
import json
import requests
import uuid
import queue
from flask import Flask, request, jsonify, Response
import logging
from datetime import datetime, timedelta
from functools import wraps

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# =============================================================================
# DEALCLOUD CONFIGURATION
# =============================================================================

DEALCLOUD_URL = os.environ.get("DEALCLOUD_URL", "https://middleground.dealcloud.com")
CLIENT_ID = os.environ.get("DEALCLOUD_CLIENT_ID", "6983")
CLIENT_SECRET = os.environ.get("DEALCLOUD_CLIENT_SECRET", "")

# Token cache
_token_cache = {
    "access_token": None,
    "expires_at": None
}

# =============================================================================
# AUTHENTICATION
# =============================================================================

def get_access_token():
    """Get or refresh DealCloud OAuth token."""
    global _token_cache
    
    # Check if token is still valid (with 60s buffer)
    if _token_cache["access_token"] and _token_cache["expires_at"]:
        if datetime.now() < _token_cache["expires_at"] - timedelta(seconds=60):
            return _token_cache["access_token"]
    
    # Request new token
    token_url = f"{DEALCLOUD_URL}/api/rest/v1/oauth/token"
    
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "data"  # Request data scope for Schema and Data endpoints
    }
    
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    
    try:
        response = requests.post(token_url, data=data, headers=headers, timeout=30)
        response.raise_for_status()
        
        token_data = response.json()
        _token_cache["access_token"] = token_data["access_token"]
        # DealCloud tokens expire in 900 seconds (15 minutes)
        _token_cache["expires_at"] = datetime.now() + timedelta(seconds=token_data.get("expires_in", 900))
        
        logger.info("Successfully obtained DealCloud access token")
        return _token_cache["access_token"]
        
    except Exception as e:
        logger.error(f"Failed to get DealCloud token: {e}")
        raise

def api_request(method, endpoint, data=None, api_version="v4"):
    """Make authenticated request to DealCloud API."""
    token = get_access_token()
    
    # Choose base path based on API version
    if api_version == "v4":
        url = f"{DEALCLOUD_URL}/api/rest/v4{endpoint}"
    else:
        url = f"{DEALCLOUD_URL}/api/rest/v1{endpoint}"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    try:
        if method == "GET":
            response = requests.get(url, headers=headers, timeout=60)
        elif method == "POST":
            response = requests.post(url, headers=headers, json=data, timeout=60)
        elif method == "PUT":
            response = requests.put(url, headers=headers, json=data, timeout=60)
        elif method == "PATCH":
            response = requests.patch(url, headers=headers, json=data, timeout=60)
        elif method == "DELETE":
            response = requests.delete(url, headers=headers, timeout=60)
        else:
            return {"error": f"Unsupported method: {method}"}
        
        response.raise_for_status()
        
        if response.text:
            return response.json()
        return {"success": True}
        
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_detail = response.json()
        except:
            error_detail = response.text
        return {"error": str(e), "detail": error_detail}
    except Exception as e:
        return {"error": str(e)}

# =============================================================================
# DEALCLOUD TOOL IMPLEMENTATIONS
# =============================================================================

def tool_list_entry_types():
    """Get all entry types in DealCloud."""
    return api_request("GET", "/schema/entryTypes", api_version="v4")

def tool_get_entry_type(entry_type_id: str):
    """Get details of a specific entry type."""
    return api_request("GET", f"/schema/entryTypes/{entry_type_id}", api_version="v4")

def tool_get_fields(entry_type_id: str):
    """Get all fields for an entry type."""
    return api_request("GET", f"/schema/entryTypes/{entry_type_id}/fields", api_version="v4")

def tool_get_field(entry_type_id: str, field_id: str):
    """Get details of a specific field."""
    return api_request("GET", f"/schema/entryTypes/{entry_type_id}/fields/{field_id}", api_version="v4")

def tool_search_entries(entry_type_id: str, query: dict = None, limit: int = 100):
    """Search entries in an entry type."""
    endpoint = f"/data/entryTypes/{entry_type_id}/entries"
    if query:
        endpoint += f"?$filter={json.dumps(query)}&$top={limit}"
    else:
        endpoint += f"?$top={limit}"
    return api_request("GET", endpoint, api_version="v4")

def tool_get_entry(entry_type_id: str, entry_id: str):
    """Get a specific entry by ID."""
    return api_request("GET", f"/data/entryTypes/{entry_type_id}/entries/{entry_id}", api_version="v4")

def tool_create_entry(entry_type_id: str, data: dict):
    """Create a new entry."""
    return api_request("POST", f"/data/entryTypes/{entry_type_id}/entries", data=data, api_version="v4")

def tool_update_entry(entry_type_id: str, entry_id: str, data: dict):
    """Update an existing entry."""
    return api_request("PATCH", f"/data/entryTypes/{entry_type_id}/entries/{entry_id}", data=data, api_version="v4")

def tool_delete_entry(entry_type_id: str, entry_id: str):
    """Delete an entry."""
    return api_request("DELETE", f"/data/entryTypes/{entry_type_id}/entries/{entry_id}", api_version="v4")

def tool_get_choice_values(field_id: str):
    """Get choice/dropdown values for a field."""
    return api_request("GET", f"/schema/fields/{field_id}/choices", api_version="v4")

def tool_get_relationships(entry_type_id: str, entry_id: str):
    """Get relationships for an entry."""
    return api_request("GET", f"/data/entryTypes/{entry_type_id}/entries/{entry_id}/relationships", api_version="v4")

def tool_test_connection():
    """Test DealCloud API connection."""
    try:
        token = get_access_token()
        entry_types = api_request("GET", "/schema/entryTypes", api_version="v4")
        return {
            "status": "connected",
            "dealcloud_url": DEALCLOUD_URL,
            "client_id": CLIENT_ID,
            "entry_types_count": len(entry_types) if isinstance(entry_types, list) else "unknown"
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}

# =============================================================================
# TOOL DEFINITIONS
# =============================================================================

TOOLS = [
    {
        "name": "test_connection",
        "description": "Test DealCloud API connection and verify credentials",
        "inputSchema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "list_entry_types",
        "description": "List all entry types (Companies, Deals, Contacts, etc.) in DealCloud",
        "inputSchema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "get_entry_type",
        "description": "Get details of a specific entry type by ID",
        "inputSchema": {
            "type": "object",
            "properties": {
                "entry_type_id": {"type": "string", "description": "The entry type ID"}
            },
            "required": ["entry_type_id"]
        }
    },
    {
        "name": "get_fields",
        "description": "Get all fields for an entry type (shows available data columns)",
        "inputSchema": {
            "type": "object",
            "properties": {
                "entry_type_id": {"type": "string", "description": "The entry type ID"}
            },
            "required": ["entry_type_id"]
        }
    },
    {
        "name": "get_field",
        "description": "Get details of a specific field including validation rules",
        "inputSchema": {
            "type": "object",
            "properties": {
                "entry_type_id": {"type": "string", "description": "The entry type ID"},
                "field_id": {"type": "string", "description": "The field ID"}
            },
            "required": ["entry_type_id", "field_id"]
        }
    },
    {
        "name": "search_entries",
        "description": "Search/filter entries in an entry type. Returns list of records.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "entry_type_id": {"type": "string", "description": "The entry type ID (e.g., 'Company', 'Deal')"},
                "query": {"type": "object", "description": "Optional filter query"},
                "limit": {"type": "integer", "description": "Max results to return (default 100)", "default": 100}
            },
            "required": ["entry_type_id"]
        }
    },
    {
        "name": "get_entry",
        "description": "Get a specific entry by ID with all field values",
        "inputSchema": {
            "type": "object",
            "properties": {
                "entry_type_id": {"type": "string", "description": "The entry type ID"},
                "entry_id": {"type": "string", "description": "The entry ID"}
            },
            "required": ["entry_type_id", "entry_id"]
        }
    },
    {
        "name": "create_entry",
        "description": "Create a new entry in DealCloud",
        "inputSchema": {
            "type": "object",
            "properties": {
                "entry_type_id": {"type": "string", "description": "The entry type ID"},
                "data": {"type": "object", "description": "Field values for the new entry"}
            },
            "required": ["entry_type_id", "data"]
        }
    },
    {
        "name": "update_entry",
        "description": "Update an existing entry in DealCloud",
        "inputSchema": {
            "type": "object",
            "properties": {
                "entry_type_id": {"type": "string", "description": "The entry type ID"},
                "entry_id": {"type": "string", "description": "The entry ID to update"},
                "data": {"type": "object", "description": "Field values to update"}
            },
            "required": ["entry_type_id", "entry_id", "data"]
        }
    },
    {
        "name": "delete_entry",
        "description": "Delete an entry from DealCloud",
        "inputSchema": {
            "type": "object",
            "properties": {
                "entry_type_id": {"type": "string", "description": "The entry type ID"},
                "entry_id": {"type": "string", "description": "The entry ID to delete"}
            },
            "required": ["entry_type_id", "entry_id"]
        }
    },
    {
        "name": "get_choice_values",
        "description": "Get dropdown/choice values for a field (e.g., deal stages, sectors)",
        "inputSchema": {
            "type": "object",
            "properties": {
                "field_id": {"type": "string", "description": "The field ID"}
            },
            "required": ["field_id"]
        }
    },
    {
        "name": "get_relationships",
        "description": "Get all relationships for an entry (linked records)",
        "inputSchema": {
            "type": "object",
            "properties": {
                "entry_type_id": {"type": "string", "description": "The entry type ID"},
                "entry_id": {"type": "string", "description": "The entry ID"}
            },
            "required": ["entry_type_id", "entry_id"]
        }
    }
]

# =============================================================================
# MCP PROTOCOL HANDLERS
# =============================================================================

def handle_initialize(params):
    return {
        "protocolVersion": "2024-11-05",
        "capabilities": {"tools": {"listChanged": False}},
        "serverInfo": {"name": "dealcloud-mcp", "version": "1.0.0"}
    }

def handle_tools_list(params):
    return {"tools": TOOLS}

def handle_tools_call(params):
    tool_name = params.get("name", "")
    arguments = params.get("arguments", {})
    
    tool_map = {
        "test_connection": lambda: tool_test_connection(),
        "list_entry_types": lambda: tool_list_entry_types(),
        "get_entry_type": lambda: tool_get_entry_type(arguments["entry_type_id"]),
        "get_fields": lambda: tool_get_fields(arguments["entry_type_id"]),
        "get_field": lambda: tool_get_field(arguments["entry_type_id"], arguments["field_id"]),
        "search_entries": lambda: tool_search_entries(
            arguments["entry_type_id"],
            arguments.get("query"),
            arguments.get("limit", 100)
        ),
        "get_entry": lambda: tool_get_entry(arguments["entry_type_id"], arguments["entry_id"]),
        "create_entry": lambda: tool_create_entry(arguments["entry_type_id"], arguments["data"]),
        "update_entry": lambda: tool_update_entry(
            arguments["entry_type_id"],
            arguments["entry_id"],
            arguments["data"]
        ),
        "delete_entry": lambda: tool_delete_entry(arguments["entry_type_id"], arguments["entry_id"]),
        "get_choice_values": lambda: tool_get_choice_values(arguments["field_id"]),
        "get_relationships": lambda: tool_get_relationships(arguments["entry_type_id"], arguments["entry_id"])
    }
    
    if tool_name not in tool_map:
        return {"content": [{"type": "text", "text": f"Unknown tool: {tool_name}"}], "isError": True}
    
    try:
        result = tool_map[tool_name]()
        return {"content": [{"type": "text", "text": json.dumps(result, indent=2, default=str)}]}
    except Exception as e:
        logger.error(f"Tool error: {e}")
        return {"content": [{"type": "text", "text": f"Error: {str(e)}"}], "isError": True}

def process_mcp_message(data):
    method = data.get("method", "")
    params = data.get("params", {})
    request_id = data.get("id", 1)
    
    logger.info(f"MCP request: {method}")
    
    if method == "initialize":
        result = handle_initialize(params)
    elif method == "tools/list":
        result = handle_tools_list(params)
    elif method == "tools/call":
        result = handle_tools_call(params)
    elif method == "notifications/initialized":
        return {"jsonrpc": "2.0", "id": request_id, "result": {}}
    else:
        return {"jsonrpc": "2.0", "id": request_id, "error": {"code": -32601, "message": f"Method not found: {method}"}}
    
    return {"jsonrpc": "2.0", "id": request_id, "result": result}

# =============================================================================
# FLASK ROUTES
# =============================================================================

sse_sessions = {}

@app.route("/", methods=["GET"])
def health_check():
    return jsonify({
        "status": "healthy",
        "service": "dealcloud-mcp",
        "version": "1.0.0",
        "dealcloud_url": DEALCLOUD_URL,
        "tools": len(TOOLS)
    })

@app.route("/mcp", methods=["POST"])
def mcp_handler():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}}), 400
        response = process_mcp_message(data)
        return jsonify(response)
    except Exception as e:
        logger.error(f"MCP handler error: {e}")
        return jsonify({"jsonrpc": "2.0", "id": 1, "error": {"code": -32603, "message": str(e)}}), 500

@app.route("/sse", methods=["GET"])
def sse_connect():
    session_id = str(uuid.uuid4())
    sse_sessions[session_id] = queue.Queue()
    logger.info(f"SSE connection established: {session_id}")
    
    def generate():
        yield f"event: endpoint\ndata: /sse/{session_id}/message\n\n"
        while True:
            try:
                try:
                    message = sse_sessions[session_id].get(timeout=30)
                    yield f"event: message\ndata: {json.dumps(message)}\n\n"
                except queue.Empty:
                    yield ": keepalive\n\n"
            except GeneratorExit:
                break
            except Exception as e:
                logger.error(f"SSE error: {e}")
                break
        if session_id in sse_sessions:
            del sse_sessions[session_id]
        logger.info(f"SSE connection closed: {session_id}")
    
    return Response(generate(), mimetype="text/event-stream",
                   headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"})

@app.route("/sse/<session_id>/message", methods=["POST"])
def sse_message(session_id):
    if session_id not in sse_sessions:
        return jsonify({"error": "Session not found"}), 404
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data"}), 400
        response = process_mcp_message(data)
        sse_sessions[session_id].put(response)
        return jsonify({"status": "ok"})
    except Exception as e:
        logger.error(f"SSE message error: {e}")
        return jsonify({"error": str(e)}), 500

# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    logger.info("DealCloud MCP Server v1.0.0 starting...")
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
