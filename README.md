🔍 ## Model Context Protocol (MCP) for Database Queries and Google Sheets output


MCP to query databases including MongoDB, PostgreSQL, MySQL, and Google BigQuery, and export the results in a Pandas Dataframe into Google Sheets. Ideal for AI Agents and data teams automating insights, data pulls, and reporting workflows.


🚀 ### Features

**🔍 Query Multiple Databases**

Execute SQL queries across:

**PostgreSQL**

**MySQL**

**BigQuery**

**MongoDB** (sql conversion)

**📤 Export to Google Sheets**
Optionally output query results (converted to Pandas Dataframe) to Google Sheets


**Requirements:**
Server hosting MCP must have network access to the databases or local access to db. For BigQuery, must have service account credentials with BigQuery permissions and BigQuery API enabled.
