# Generate tools for all SHOW and DESCRIBE commands
show_objects = [
    "ALERTS",
    "APPLICATION ROLES",
    "APPLICATIONS",
    "COLUMNS",
    "CONNECTIONS",
    "DATABASES",
    "EXTERNAL FUNCTIONS",
    "EXTERNAL TABLES",
    "FUNCTIONS",
    "GRANTS",
    "INTEGRATIONS",
    "LOCKS",
    "MANAGED ACCOUNTS",
    "MATERIALIZED VIEWS",
    "NETWORK POLICIES",
    "OBJECTS",
    "PARAMETERS",
    "PIPES",
    "PROCEDURES",
    "REGIONS",
    "REPLICATION DATABASES",
    "REPLICATION GROUPS",
    "ROLES",
    "SCHEMAS",
    "SEQUENCES",
    "SERVICES",
    "SHARES",
    "STAGES",
    "STREAMS",
    "TABLES",
    "TASKS",
    "TRANSACTIONS",
    "USERS",
    "VIEWS",
    "WAREHOUSES"
]

describe_objects = [
    "ACCOUNT",
    "ALERT",
    "APPLICATION",
    "APPLICATION ROLE",
    "COLUMN",
    "CONNECTION",
    "DATABASE",
    "EXTERNAL FUNCTION",
    "EXTERNAL TABLE",
    "FUNCTION",
    "INTEGRATION",
    "MATERIALIZED VIEW",
    "NETWORK POLICY",
    "PIPE",
    "PROCEDURE",
    "REPLICATION DATABASE",
    "REPLICATION GROUP",
    "ROLE",
    "SCHEMA",
    "SEQUENCE",
    "SERVICE",
    "SHARE",
    "STAGE",
    "STREAM",
    "TABLE",
    "TASK",
    "USER",
    "VIEW",
    "WAREHOUSE"
]

all_tools = []

for obj in show_objects:
    # Handle multi-word object types (e.g., "APPLICATION ROLES")
    obj_type = obj.replace(" ", "_")
    show_obj_lower = obj.lower().replace(" ", "_")
    
    tool_code = f'''
@server.tool("show_{show_obj_lower}")
def show_{show_obj_lower}() -> List[Dict[str, Any]]:
    """
    List all {show_obj_lower} in Snowflake.
    
    Returns:
        List of {show_obj_lower} with their details
    """
    logger.info(f"Listing all {show_obj_lower}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list {show_obj_lower}.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW {obj_type}")
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing {show_obj_lower}: {{e}}")
        raise
    finally:
        return_connection_to_pool(conn)
'''
    all_tools.append(tool_code)

# Join all tools with double newlines for separation
complete_tools_code = "\n\n".join(all_tools)


for obj in describe_objects:
    # Handle multi-word object types (e.g., "APPLICATION ROLES")
    obj_type = obj.replace(" ", "_")
    describe_obj_lower = obj.lower().replace(" ", "_")
    
    tool_code = f'''
@server.tool("describe_{describe_obj_lower}")
def describe_{describe_obj_lower}(name: str) -> Dict[str, Any]:
    """
    Get details about a specific {describe_obj_lower}.
    
    Args:
        name: Name of the {describe_obj_lower} to describe
        
    Returns:
        Detailed information about the {describe_obj_lower}
    """
    logger.info(f"Describing {describe_obj_lower}: {{name}}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe {describe_obj_lower}.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(f"DESCRIBE {obj_type} {{name}}")
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing {describe_obj_lower} {{name}}: {{e}}")
        raise
    finally:
        return_connection_to_pool(conn)
'''
    all_tools.append(tool_code)

# Join all tools with double newlines for separation
complete_tools_code = "\n\n".join(all_tools)        
with open("snowflake_tools_list.txt", "w") as f:
    f.write(complete_tools_code)