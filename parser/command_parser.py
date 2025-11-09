import re

class CommandParser:
    def parse(self, command: str):
        command = command.strip().rstrip(";")
        upper = command.upper()

        # SHOW TABLES
        if upper.startswith("SHOW TABLES"):
            return {"type": "SHOW_TABLES"}

        # SHOW COLUMNS FROM table
        if upper.startswith("SHOW COLUMNS FROM"):
            match = re.match(r"SHOW\s+COLUMNS\s+FROM\s+(\w+)", command, re.IGNORECASE)
            if not match:
                raise Exception("Invalid SHOW COLUMNS syntax.")
            return {"type": "SHOW_COLUMNS", "name": match.group(1)}

        # DROP TABLE
        if upper.startswith("DROP TABLE"):
            match = re.match(r"DROP\s+TABLE\s+(\w+)", command, re.IGNORECASE)
            if not match:
                raise Exception("Invalid DROP TABLE syntax.")
            return {"type": "DROP_TABLE", "name": match.group(1)}

        # TRUNCATE TABLE users
        if upper.startswith("TRUNCATE TABLE"):
            match = re.match(r"TRUNCATE\s+TABLE\s+(\w+)", command, re.IGNORECASE)
            if not match:
                raise Exception("Invalid TRUNCATE TABLE syntax.")
            return {"type": "TRUNCATE_TABLE", "name": match.group(1)}

        # CREATE TABLE users (id INT, name TEXT)
        if upper.startswith("CREATE TABLE"):
            match = re.match(r"CREATE\s+TABLE\s+(\w+)\s*\((.+)\)", command, re.IGNORECASE)
            if not match:
                raise Exception("Invalid CREATE TABLE syntax.")
            name = match.group(1)
            cols_raw = match.group(2)
            parts = [p.strip() for p in cols_raw.split(",")]
            columns = []
            for p in parts:
                col_parts = p.split()
                if len(col_parts) != 2:
                    raise Exception(f"Invalid column definition: {p}")
                columns.append((col_parts[0], col_parts[1].upper()))
            return {"type": "CREATE", "name": name, "columns": columns}

        # INSERT INTO tableName VALUES ()
        if upper.startswith("INSERT INTO"):
            match = re.match(r"INSERT\s+INTO\s+(\w+)\s+VALUES\s*\((.+)\)", command, re.IGNORECASE)
            if not match:
                raise Exception("Invalid INSERT syntax.")
            name = match.group(1)
            values_raw = match.group(2)
            parts = re.findall(r'"[^"]*"|\'[^\']*\'|[^,]+', values_raw)
            values = [p.strip().strip('"').strip("'") for p in parts]
            return {"type": "INSERT", "name": name, "values": values}

        # SELECT name FROM users WHERE id = 1
        if upper.startswith("SELECT"):
            match = re.match(r"SELECT\s+(.+)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?", command, re.IGNORECASE)
            if not match:
                raise Exception("Invalid SELECT syntax.")
            cols_part, table_name, where_clause = match.groups()
            columns = None if cols_part.strip() == "*" else [c.strip() for c in cols_part.split(",")]
            where = None
            if where_clause:
                where_match = re.match(r"(\w+)\s*(=|!=)\s*(.+)", where_clause)
                if where_match:
                    col, op, val = where_match.groups()
                    val = val.strip().strip('"').strip("'")
                    where = (col, op, val)
            return {"type": "SELECT", "name": table_name, "columns": columns, "where": where}

        # UPDATE users SET name = "Alicia" WHERE id = 2
        if upper.startswith("UPDATE"):
            match = re.match(r"UPDATE\s+(\w+)\s+SET\s+(\w+)\s*=\s*(.+?)\s+WHERE\s+(\w+)\s*(=|!=)\s*(.+)", command, re.IGNORECASE)
            if not match:
                raise Exception("Invalid UPDATE syntax.")
            name, set_col, set_val, where_col, op, where_val = match.groups()
            set_val = set_val.strip().strip('"').strip("'")
            where_val = where_val.strip().strip('"').strip("'")
            return {
                "type": "UPDATE",
                "name": name,
                "set": (set_col, set_val),
                "where": (where_col, op, where_val)
            }

        # DELETE FROM users WHERE id = 1
        if upper.startswith("DELETE FROM"):
            match = re.match(r"DELETE\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s*(=|!=)\s*(.+)", command, re.IGNORECASE)
            if not match:
                raise Exception("Invalid DELETE syntax.")
            name, where_col, op, where_val = match.groups()
            where_val = where_val.strip().strip('"').strip("'")
            return {"type": "DELETE", "name": name, "where": (where_col, op, where_val)}

        raise Exception("Unknown or unsupported command.")
