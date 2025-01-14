from abc import ABC, abstractmethod


class MsgpackSerializable(ABC):
    @abstractmethod
    def __msgpack__(self): ...


class JSONSerializable(ABC):
    @abstractmethod
    def __json__(self): ...
