import oracledb
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("oracle")

DSN = "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.0.40.87)(PORT=1522))(CONNECT_DATA=(SERVER=DEDICATED)(SID=CMIS3)))"

def get_connection(schema_name: str):
    schema = schema_name.upper().strip()
    password = f"{schema}DB27"
    return oracledb.connect(user=schema, password=password, dsn=DSN)

@mcp.tool()
def oracle_list_tables(schema_name: str) -> str:
    """List all tables belonging to a specific schema (HOPDONG, CHISO, or CHISODX)."""
    try:
        schema = schema_name.upper().strip()
        with get_connection(schema) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT table_name FROM user_tables ORDER BY table_name")
                tables = [row[0] for row in cursor.fetchall()]
                return f"Tables in schema {schema}:\n" + "\n".join(tables)
    except Exception as e:
        return f"Error listing tables for schema {schema_name}: {str(e)}"

@mcp.tool()
def oracle_describe_table(schema_name: str, table_name: str) -> str:
    """Get the column schema and structure of a specific table in a schema (HOPDONG, CHISO, or CHISODX)."""
    try:
        schema = schema_name.upper().strip()
        table = table_name.upper().strip()
        with get_connection(schema) as conn:
            with conn.cursor() as cursor:
                sql = """
                SELECT column_name, data_type, data_length, nullable 
                FROM user_tab_columns 
                WHERE table_name = :tbl 
                ORDER BY column_id
                """
                cursor.execute(sql, tbl=table)
                cols = cursor.fetchall()
                if not cols:
                    return f"Table {schema}.{table} not found."
                
                result = [f"STRUCTURE OF ORACLE TABLE: {schema}.{table}"]
                result.append("-" * 75)
                result.append(f"{'Column Name':<30} | {'Data Type':<18} | {'Length':<10} | {'Nullable':<8}")
                result.append("-" * 75)
                for col in cols:
                    result.append(f"{col[0]:<30} | {col[1]:<18} | {str(col[2]):<10} | {col[3]:<8}")
                result.append("-" * 75)
                return "\n".join(result)
    except Exception as e:
        return f"Error describing table {schema_name}.{table_name}: {str(e)}"

@mcp.tool()
def oracle_execute_query(schema_name: str, sql: str, limit: int = 50) -> str:
    """Execute a read-only SELECT query on a specific schema (HOPDONG, CHISO, or CHISODX) and return rows."""
    if not sql.strip().upper().startswith("SELECT"):
        return "Error: Only SELECT queries are allowed."
    try:
        schema = schema_name.upper().strip()
        with get_connection(schema) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                cols = [desc[0] for desc in cursor.description]
                rows = cursor.fetchmany(limit)
                
                result = []
                result.append(" | ".join(cols))
                result.append("-" * 80)
                for row in rows:
                    result.append(" | ".join(str(val) for val in row))
                return "\n".join(result)
    except Exception as e:
        return f"Error executing query on schema {schema_name}: {str(e)}"

if __name__ == "__main__":
    mcp.run()
