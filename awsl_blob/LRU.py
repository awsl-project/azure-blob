from collections import OrderedDict


class LRU(object):
    """
    LRU Cache
    """
    def __init__(self, cache_size=128):
        self.cache_size = cache_size
        self.cache = OrderedDict()

    def get(self, key, default):
        """
        获取 key 并更新到最后
        """
        res = self.cache.pop(key, None)
        if res:
            self.cache[key] = res
        return res if res is not None else default

    def put(self, key, value):
        """
        更新 key 并放到最后
        长度上限时移除头部
        """
        res = self.cache.pop(key, None)
        if not res and len(self.cache) == self.cache_size:
            self.cache.popitem(False)
        self.cache[key] = value
