"""
abstract base class work

"""

from abc import ABC


class ADataLoader(ABC):

    # abstract method
    def populateTable(self, config, DS):
        pass
