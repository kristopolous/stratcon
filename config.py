import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

FALKORDB_URL = os.environ["FALKORDB_URL"]
BRIGHT_DATA_KEY = os.environ["BRIGHT_DATA_KEY"]
SYSTEMONE_URL = os.environ["SYSTEMONE_URL"]
SCORE_MODEL = os.environ.get("SCORE_MODEL", "clef-flash")
ROUTER_MODEL = os.environ.get("ROUTER_MODEL", "nimble-v3")
LLM_BASE_URL = os.environ["LLM_BASE_URL"]
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
LLM_MODEL = os.environ.get("LLM_MODEL", "qwen3.8")
UNLOCKER_ZONE = os.environ.get("UNLOCKER_ZONE", "mcp_unlocker")
BROWSER_ZONE = os.environ.get("BROWSER_ZONE", "mcp_browser")
GRAPH = os.environ.get("GRAPH", "stratcon")
TRUEFORGE_URL = os.environ.get("TRUEFORGE_URL", "")
TRUEFORGE_TOKEN = os.environ.get("TRUEFORGE_TOKEN", "")
TRUEFORGE_MODEL = os.environ.get("TRUEFORGE_MODEL", "")
PUBLIC_URL = os.environ.get("PUBLIC_URL", "")
MCP_NAME = os.environ.get("MCP_NAME", "stratcon")
MCP_TOKEN = os.environ.get("MCP_TOKEN", "")
MCP_URL = os.environ.get("MCP_URL", "http://localhost:8000/tools/mcp")
