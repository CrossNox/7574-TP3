from typing import Any, Dict, List, Optional

Entry = Dict[str, Any]


class Joiner:
    def __init__(self, columns: List[str]):
        self.columns = set(columns)
        self.table: Dict[str, Entry] = {}

    def add(self, entry):
        if "id" in entry and entry["id"] not in self.table:
            self.table[entry["id"]] = entry

    def join(self, entry) -> Optional[Entry]:
        if "id" in entry and entry["id"] in self.table:
            cpy = dict(self.table[entry["id"]])
            cpy.update(entry)
            ret = {k: v for k, v in cpy.items() if k in self.columns}
            return ret
        else:
            return None

    def reset(self):
        self.table = {}
