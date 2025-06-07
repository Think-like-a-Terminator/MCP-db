import asyncio
from mcp.shared.memory import (
    create_connected_server_and_client_session as client_session,
)
from mcp.types import TextContent, TextResourceContents
from pydantic import AnyUrl


async def test_connection():
    """Test the mcp server by querying BigQuery database."""
    from main import mcp

    async with client_session(mcp._mcp_server) as client:
        # list available tools in the MCP server
        result = await client.read_resource("config://app")
        content = result.contents[0]
        print(content.text)

        # call the query_database tool to query BigQuery, set the enviornment variable in .env file
        result = await client.call_tool("query_database", {"params": {
            "db_type": "BigQuery", 
            "host": "localhost", # BigQuery does not require a host, can be set to localhost
            "port": 5432,  # BigQuery does not require a port, can be set to any value
            "db_name": "", # your BigQuery Dataset ID/name 
            "username": "user", # BigQuery does not require a username, can be set to any value
            "password": "password",  # BigQuery does not require a password, can be set to any value
            "sql_query": "SELECT * FROM `DATASETID.TABLENAME` LIMIT 20"
            }}
        )
        content = result.content[0]
        print(content.text)


if __name__ == "__main__":
    asyncio.run(test_connection())

