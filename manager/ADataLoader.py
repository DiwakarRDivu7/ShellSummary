"""
abstract base class work

"""

from abc import ABC, abstractmethod

class ADataLoader(ABC):

    # abstract method
    def populateTable(self):
        pass