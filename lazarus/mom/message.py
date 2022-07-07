import json


class Message:
    def __init__(self, data=None):
        self._data = data or {}

    def __contains__(self, key):
        return key in self._data

    @property
    def data(self):
        return self._data

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        self._data[key] = value

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __str__(self):
        return str(self._data)

    def __repr__(self):
        return f"Message with data {self._data}"

    # Check queue.py/consume if you are confused
    def ack(self):
        raise NotImplementedError("ack was called without an implementation")

    def nack(self):
        raise NotImplementedError("nack was called without an implementation")

    def encode(self) -> bytes:
        return json.dumps(self._data).encode()

    @classmethod
    def decode(cls, data: bytes):
        m = Message()
        m._data = json.loads(data.decode())
        return m
