from typing import Optional


class Joiner:
    def __init__(self):
        self.table = {}

    def add(self, entry):
        if "id" in entry and entry["id"] not in self.table:
            self.table[entry["id"]] = entry

    def join(self, entry) -> Optional[str]:
        if "id" in entry and entry["id"] in self.table:
            cpy = dict(self.table[entry["id"]])
            cpy.update(entry)
            return cpy
        else:
            return None
