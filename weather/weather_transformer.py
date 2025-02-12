from abc import ABC, abstractmethod

class Transformer(ABC):
    @abstractmethod
    def get_transform(self):
        """transform method varies depending on weather source"""
        ...
