"""Register StratCon's model and MCP server with TrueForge on TrueFoundry, then list what the agent can use."""
import asyncio
import re

import trueforge_sdk as t

import config

PROVIDER = "stratcon-qwen"


async def main():
    c = t.AsyncTrueForge(base_url=config.TRUEFORGE_URL, token=config.TRUEFORGE_TOKEN or None, timeout=60)
    await c.settings.model_providers.create_or_update(manifest=t.CustomModelProvider(
        type="custom", name=PROVIDER, base_url=config.LLM_BASE_URL,
        auth=t.ModelProviderAuth(api_key=config.LLM_API_KEY or "none"),
        models=[t.ConfiguredModel(model_id=config.LLM_MODEL, name=re.sub(r"[^a-z0-9-]", "-", config.LLM_MODEL.lower()),
                                  properties=t.ModelProperties(context_length=32768, max_output_tokens=2048))]))
    print(f"model provider '{PROVIDER}' -> {config.LLM_MODEL}")

    await c.settings.mcp_servers.create_or_update(manifest=t.RemoteMcpServerManifest(
        type="remote", name=config.MCP_NAME, url=config.MCP_URL,
        description="StratCon rubric tools: edit criteria, set scope, set the conference clock, export a calendar.",
        auth=t.McpServerHeaderAuth(type="header", headers={"x-stratcon-token": config.MCP_TOKEN})))
    print(f"mcp server '{config.MCP_NAME}' -> {config.MCP_URL}")

    models = await c.models.list()
    print("models visible to agents:")
    for m in getattr(models, "data", models) or []:
        print("  ", getattr(m, "name", m))
    servers = await c.mcp_servers.list()
    print("mcp servers visible to agents:")
    for s in getattr(servers, "data", servers) or []:
        print("  ", getattr(s, "name", s))


if __name__ == "__main__":
    asyncio.run(main())
