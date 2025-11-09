import os
import json
from storage.bplustree import BPlusTree

class StorageEngine:
    def __init__(self, base_path="data"):
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)
        self.indexes = {}  # table_name -> column -> BPlusTree

    def _table_path(self, name):
        return os.path.join(self.base_path, f"{name}.json")

    def _load_table(self, name):
        path = self._table_path(name)
        if not os.path.exists(path):
            raise Exception(f"Table '{name}' not found.")
        with open(path, "r") as f:
            return json.load(f)

    def _save_table(self, name, table):
        with open(self._table_path(name), "w") as f:
            json.dump(table, f)

    def create_table(self, name, columns):
        path = self._table_path(name)
        if os.path.exists(path):
            raise Exception(f"Table '{name}' already exists.")
        table = {"columns": [{"name": c[0], "type": c[1]} for c in columns], "rows": []}
        self._save_table(name, table)
        print(f"✅ Created table '{name}' with columns {columns}")

    def drop_table(self, name):
        path = self._table_path(name)
        if not os.path.exists(path):
            raise Exception(f"Table '{name}' not found.")
        os.remove(path)
        # Also remove indexes if any exist
        for f in os.listdir(self.base_path):
            if f.startswith(f"{name}_") and f.endswith("_index.json"):
                os.remove(os.path.join(self.base_path, f))
        print(f"🗑️  Dropped table '{name}' and its indexes.")

    def truncate_table(self, name):
        path = self._table_path(name)
        if not os.path.exists(path):
            raise Exception(f"Table '{name}' not found.")
        with open(path, "r") as f:
            table = json.load(f)
        table["rows"] = []
        self._save_table(name, table)
        print(f"🧹 Truncated table '{name}' (all rows deleted, schema kept).")
        
    def insert(self, name, values):
        table = self._load_table(name)
        if len(values) != len(table["columns"]):
            raise Exception("Value count doesn’t match column count.")

        typed_values = []
        for val, col in zip(values, table["columns"]):
            ctype = col["type"]
            if ctype == "INT":
                val = int(val)
            typed_values.append(val)
        table["rows"].append(typed_values)
        self._save_table(name, table)

        # Build B+ Tree index for first column (id)
        key_col = table["columns"][0]["name"]
        key_val = typed_values[0]
        index_file = f"{self.base_path}/{name}_{key_col}_index.json"
        if name not in self.indexes:
            self.indexes[name] = {}
        if key_col not in self.indexes[name]:
            self.indexes[name][key_col] = BPlusTree(order=4, persist_file=index_file)
        self.indexes[name][key_col].insert(key_val, typed_values)
        print(f"✅ Inserted {typed_values} into '{name}'")


    def select(self, name, columns=None, where=None):
        table = self._load_table(name)
        rows = table["rows"]

        if where:
            col, op, val = where
            idx = next(i for i, c in enumerate(table["columns"]) if c["name"] == col)
            if table["columns"][idx]["type"] == "INT":
                val = int(val)

            if op == "=" and name in self.indexes and col in self.indexes[name]:
                result = self.indexes[name][col].search(val)
                rows = [result] if result else []
            elif op in [">", "<"]:
                all_pairs = self.indexes[name][col].range_search(
                    low=-float("inf") if op == "<" else val,
                    high=float("inf") if op == ">" else val,
                )
                rows = [v for _, v in all_pairs]
            else:
                if op == "=":
                    rows = [r for r in rows if r[idx] == val]
                elif op == "!=":
                    rows = [r for r in rows if r[idx] != val]

        if columns:
            col_indices = [i for i, c in enumerate(table["columns"]) if c["name"] in columns]
            rows = [[r[i] for i in col_indices] for r in rows]
        return rows


    def update(self, name, set_clause, where):
        table = self._load_table(name)
        set_col, set_val = set_clause
        where_col, op, where_val = where
        set_idx = next(i for i, c in enumerate(table["columns"]) if c["name"] == set_col)
        where_idx = next(i for i, c in enumerate(table["columns"]) if c["name"] == where_col)
        count = 0
        for r in table["rows"]:
            if op == "=" and str(r[where_idx]) == str(where_val):
                r[set_idx] = set_val
                count += 1
        self._save_table(name, table)
        self._update_index(name, table)
        print(f"✅ Updated {count} rows in '{name}'")

    def delete(self, name, where):
        table = self._load_table(name)
        where_col, op, where_val = where
        idx = next(i for i, c in enumerate(table["columns"]) if c["name"] == where_col)
        before = len(table["rows"])
        table["rows"] = [r for r in table["rows"] if str(r[idx]) != str(where_val)]
        after = len(table["rows"])
        self._save_table(name, table)
        self._update_index(name, table)
        print(f"✅ Deleted {before - after} rows from '{name}'")

    def show_tables(self):
        return [f[:-5] for f in os.listdir(self.base_path) if f.endswith(".json")]

    def show_columns(self, name):
        table = self._load_table(name)
        return [(c["name"], c["type"]) for c in table["columns"]]

    def _update_index(self, name, table):
        """Simple in-memory index for fast lookups."""
        self.indexes[name] = {}
        for i, col in enumerate(table["columns"]):
            col_name = col["name"]
            self.indexes[name][col_name] = {}
            for row_idx, row in enumerate(table["rows"]):
                val = row[i]
                self.indexes[name][col_name].setdefault(val, []).append(row_idx)
