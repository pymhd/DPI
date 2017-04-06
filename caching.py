import os
from collections import OrderedDict

#use move_to_end method in Python3 instead of stupid pop and add

class Cache:
    def __init__(self, cacheSize):
        self.cache = OrderedDict()
        self.cacheSize = cacheSize

    def __getitem__(self, key):
        # catch key error in module, not here
        value = self.cache[key]
        return self.touchCache(key, value)

    def __setitem__(self, key, value):
        self.touchCache(key, value)

    def touchCache(self, key, value):
        try: 
            self.cache.pop(key)
            self.cache[key] = value
            return value
        except KeyError:
            self.cache[key] = value
            if len(self.cache.keys()) > self.cacheSize:
                self.cache.pop(self.cache.keys()[0])
            return value
