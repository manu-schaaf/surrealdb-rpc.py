from abc import ABC, abstractmethod


class SurrealQLSerializable(ABC):
    @abstractmethod
    def __surql__(self) -> str: ...


class JSONSerializable(ABC):
    @abstractmethod
    def __json__(self) -> str: ...
