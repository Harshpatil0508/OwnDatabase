import json
import bisect
import os

class Node:
    def __init__(self, is_leaf=False):
        self.is_leaf = is_leaf
        self.keys = []
        self.children = []  # pointers or values
        self.next = None    # link between leaves

class BPlusTree:
    def __init__(self, order=4, persist_file=None):
        self.root = Node(is_leaf=True)
        self.order = order
        self.persist_file = persist_file
        if persist_file and os.path.exists(persist_file):
            self._load()

    # --- Core operations ---
    def search(self, key):
        node = self.root
        while not node.is_leaf:
            idx = bisect.bisect_right(node.keys, key)
            node = node.children[idx]
        for i, k in enumerate(node.keys):
            if k == key:
                return node.children[i]
        return None

    def insert(self, key, value):
        split = self._insert_recursive(self.root, key, value)
        if split:
            key_up, right = split
            new_root = Node(is_leaf=False)
            new_root.keys = [key_up]
            new_root.children = [self.root, right]
            self.root = new_root
        self._save()

    def _insert_recursive(self, node, key, value):
        if node.is_leaf:
            idx = bisect.bisect_left(node.keys, key)
            node.keys.insert(idx, key)
            node.children.insert(idx, value)
            if len(node.keys) > self.order:
                return self._split_node(node)
            return None
        else:
            idx = bisect.bisect_right(node.keys, key)
            split = self._insert_recursive(node.children[idx], key, value)
            if split:
                key_up, right = split
                node.keys.insert(idx, key_up)
                node.children.insert(idx + 1, right)
                if len(node.keys) > self.order:
                    return self._split_node(node)
            return None

    def _split_node(self, node):
        mid = len(node.keys) // 2
        key_up = node.keys[mid]

        right = Node(is_leaf=node.is_leaf)
        right.keys = node.keys[mid + (0 if node.is_leaf else 1):]
        right.children = node.children[mid + 1:] if not node.is_leaf else node.children[mid:]

        node.keys = node.keys[:mid] if not node.is_leaf else node.keys[:mid]
        node.children = node.children[:mid + 1] if not node.is_leaf else node.children[:mid]

        if node.is_leaf:
            right.next = node.next
            node.next = right

        return key_up, right

    # --- Persistence ---
    def _save(self):
        if not self.persist_file:
            return
        with open(self.persist_file, "w") as f:
            json.dump(self._serialize_node(self.root), f)

    def _load(self):
        with open(self.persist_file, "r") as f:
            data = json.load(f)
            self.root = self._deserialize_node(data)

    def _serialize_node(self, node):
        if node is None:
            return None
        return {
            "is_leaf": node.is_leaf,
            "keys": node.keys,
            "children": [self._serialize_node(c) if isinstance(c, Node) else c for c in node.children]
        }

    def _deserialize_node(self, data):
        node = Node(is_leaf=data["is_leaf"])
        node.keys = data["keys"]
        node.children = [
            self._deserialize_node(c) if isinstance(c, dict) else c
            for c in data["children"]
        ]
        return node

    # --- Range query ---
    def range_search(self, low, high):
        node = self.root
        while not node.is_leaf:
            node = node.children[0]
        results = []
        while node:
            for k, v in zip(node.keys, node.children):
                if low <= k <= high:
                    results.append((k, v))
            node = node.next
        return results
