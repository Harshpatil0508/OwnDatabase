from storage.storage_engine import StorageEngine
from parser.command_parser import CommandParser

def main():
    engine = StorageEngine()
    parser = CommandParser()
    print("Welcome to MiniDB v0.4")
    print("Type 'exit' to quit.\n")

    while True:
        cmd = input("mydb> ").strip()
        if cmd.lower() in ["exit", "quit"]:
            print("👋 Goodbye!")
            break
        if not cmd:
            continue

        try:
            parsed = parser.parse(cmd)
            t = parsed["type"]

            if t == "CREATE":
                engine.create_table(parsed["name"], parsed["columns"])
            elif t == "DROP_TABLE":
                engine.drop_table(parsed["name"])
            elif t == "TRUNCATE_TABLE":
                engine.truncate_table(parsed["name"])
            elif t == "INSERT":
                engine.insert(parsed["name"], parsed["values"])
            elif t == "SELECT":
                rows = engine.select(parsed["name"], parsed.get("columns"), parsed.get("where"))
                print(rows)
            elif t == "UPDATE":
                engine.update(parsed["name"], parsed["set"], parsed["where"])
            elif t == "DELETE":
                engine.delete(parsed["name"], parsed["where"])
            elif t == "SHOW_TABLES":
                tables = engine.show_tables()
                print("📋 Tables:")
                for tname in tables:
                    print("  -", tname)
            elif t == "SHOW_COLUMNS":
                columns = engine.show_columns(parsed["name"])
                print("📋 Columns:")
                for cname, ctype in columns:
                    print(f"  - {cname} ({ctype})")
            else:
                print("Unknown command.")
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
