# Usage: python test.py
# Runs a sequence of MCP tool calls against the Microscope server.
# Uncomment sections as needed for troubleshooting.

import uuid
import yaml
import asyncio
from fastmcp import Client
import time 


lattice_config = "polygon"


# HTTP server (Update Port if Needed)
client = Client("http://localhost:8000/mcp")


async def main():
    async with client:

        # Health check
        await client.ping()
        print("Ping OK")

        # Invoke a topology
        invoke_id = str(uuid.uuid4())
        invoke = await client.call_tool("invoke_topology", {"invoke_id": invoke_id, "lattice_id": lattice_config})
        print("Invoke:", invoke)

        time.sleep(1)
        
        # Get Invoke Status 
        invoke_status = await client.call_tool("get_invoke_status", {"invoke_id": invoke_id})
        print("Invoke Status:", invoke_status)


asyncio.run(main())