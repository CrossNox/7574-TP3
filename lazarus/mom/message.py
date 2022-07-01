import json


class Message:
    def __init__(self):
        self._data = {}

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        self._data[key] = value

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
