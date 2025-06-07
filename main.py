import json
import logging
import socket
from mcp.server.fastmcp import FastMCP
import psycopg2, pymysql, pymongo
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime
from typing import Any, Dict, List
from dotenv import load_dotenv
import os
from utils import write_dataframe_to_sheet

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Initialize MCP server
mcp = FastMCP(
    name="DBConnectionAgent"
    )


@mcp.resource("config://app")
def list_tools():
    return {
        "tools": [
            {
                "name": name,
                "parameters": schema["parameters"],
                "description": schema["description"]
            }
            for name, schema in tool_schemas.items()
        ]
    }

tool_schemas = {}

def tool_schema(name: str, params: Dict[str, Any], description: str = ""):
    def decorator(func):
        tool_schemas[name] = {
            "parameters": params,
            "description": description,
            "function": func
        }
        return func
    return decorator


@tool_schema(
    name="test_db_connection",
    params={
        "db_type": {"type": "string", "required": True, 
                    "description": "Database type (PostgreSQL, \
                    MySQL, MongoDB, BigQuery)"
                },
        "host": {"type": "string", "required": True},
        "port": {"type": "integer", "required": False},
        "db_name": {"type": "string", "required": True},
        "username": {"type": "string", "required": True},
        "password": {"type": "string", "required": True}
    },
    description="Test connection to a database. \
                Database types are PostgreSQL, \
                MySQL, MongoDB, BigQuery."
)

@mcp.tool()
def test_db_connection(params: dict) -> dict:
    db_type = params.get("db_type")
    host = params.get("host")
    port = params.get("port")
    db_name = params.get("db_name")
    username = params.get("username")
    password = params.get("password")
    credentials_path = params.get("credentials_path")

    try:
        if str(db_type).lower() == "postgresql":
            conn = psycopg2.connect(
                host=host,
                port=port or 5432,
                dbname=db_name,
                user=username,
                password=password
            )
            conn.close()

        elif str(db_type).lower() == "mysql":
            conn = pymysql.connect(
                host=host,
                port=port or 3306,
                user=username,
                password=password,
                database=db_name
            )
            conn.close()

        elif str(db_type).lower() == "mongodb":
            port = port or 27017
            uri = f"mongodb://{username}:{password}@{host}:{port}"
            client = pymongo.MongoClient(uri)
            client.admin.command('ping')

        elif str(db_type).lower() == "bigquery":
            credentials_path = os.getenv("GOOGLE_CREDS_PATH")
            client = bigquery.Client.from_service_account_json(credentials_path)
            result = client.query(f"SELECT 1 FROM {db_name}").result()

        else:
            return {"success": False, "message": "Unsupported database type."}

        return {"success": True, "result": result, "message": "Connection successful"}

    except Exception as e:
        return {"success": False, "message": f"Connection failed: {str(e)}"}


@tool_schema(
    name="query_database",
    params={
        "db_type": {"type": "string", "required": True},
        "host": {"type": "string", "required": True},
        "port": {"type": "integer", "required": False},
        "db_name": {"type": "string", "required": True},
        "username": {"type": "string", "required": True},
        "password": {"type": "string", "required": True},
        "sql_query": {"type": "string", "required": True},
        "output_to_sheet": {"type": "boolean", "required": False},
        "output_sheet": {"type": "string", "required": False},
        "sheet_tab": {"type": "string", "required": False}
    },
    description="Query a database and optionally write output to Google Sheet."
)

@mcp.tool()
def query_database(params: dict) -> dict:
    db_type = params.get("db_type")
    host = params.get("host")
    port = params.get("port")
    db_name = params.get("db_name")
    username = params.get("username")
    password = params.get("password")
    credentials_path = params.get("credentials_path")
    sql_query = params.get("sql_query")
    output_to_sheet = params.get("output_to_sheet", False)
    if output_to_sheet:
        if "output_sheet" not in params or "sheet_tab" not in params:
            return {"success": False, "message": "output_to_sheet is True but \
                        output_sheet or sheet_tab param is missing."}
        output_sheet = params.get("output_sheet")
        sheet_tab = params.get("sheet_tab")

    try:
        if str(db_type).lower() == "postgresql":
            conn = psycopg2.connect(
                host=host,
                port=port or 5432,
                dbname=db_name,
                user=username,
                password=password
            )
            df = pd.read_sql_query(sql_query, conn)
            conn.close()
            if output_to_sheet:
                write_dataframe_to_sheet(df, sheetname=output_sheet, tabname=sheet_tab)
            

        elif str(db_type).lower() == "mysql":
            conn = pymysql.connect(
                host=host,
                port=port or 3306,
                user=username,
                password=password,
                database=db_name
            )
            df = pd.read_sql_query(sql_query, conn)
            conn.close()
            if output_to_sheet:
                write_dataframe_to_sheet(df, sheetname=output_sheet, tabname=sheet_tab)
            
        elif str(db_type).lower() == "mongodb":
            port = port or 27017
            uri = f"mongodb://{username}:{password}@{host}:{port}"
            client = pymongo.MongoClient(uri)
            db = client[db_name]
            collection = db[db_name]
            cursor = collection.aggregate([{"$match": {"$expr": {"$eq": ["$query", sql_query]}}}])
            df = pd.DataFrame(list(cursor))
            client.close()
            if output_to_sheet:
                write_dataframe_to_sheet(df, sheetname=output_sheet, tabname=sheet_tab)

        elif str(db_type).lower() == "bigquery":
            credentials_path = os.getenv("GOOGLE_CREDS_PATH")
            client = bigquery.Client.from_service_account_json(credentials_path)
            df = client.query(sql_query).to_dataframe()
            if output_to_sheet:
                write_dataframe_to_sheet(df, sheetname=output_sheet, tabname=sheet_tab)

        else:
            return {"success": False, "message": "Unsupported db_type for SQL query."}

        # Convert dataframe to dict of records
        records = df.to_dict(orient="records")
        return {"success": True, "data": records}

    except Exception as e:
        return {"success": False, "message": f"Query failed: {str(e)}"}



if __name__ == "__main__":
    logger.info("Starting FastMCP server")
    mcp.run()
