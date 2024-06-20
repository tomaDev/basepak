import functools
import json
import os
import time

CACHE_DIR = f'{os.path.expanduser("~")}/.basepak/cache'


# todo: use and test this
def fcache(ttl=12 * 60 * 60):
    """Cache function results in a file"""
    def _cache(func):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            cache_key = func.__qualname__ + '-' + (
                '_'.join(map(str, sum(sorted(kwargs.items()), tuple()) + args)).replace('/', '-')
            )
            cache_file = os.path.join(CACHE_DIR, cache_key)
            now = time.time()
            if os.path.exists(cache_file):
                with open(cache_file) as cache_content:
                    content = json.load(cache_content)
                    if (now - content['created']) < ttl:
                        return content['content']
            result = func(*args, **kwargs)
            os.makedirs(CACHE_DIR, exist_ok=True)
            with open(cache_file, 'w') as cache_content:
                json.dump({'content': result, 'created': now}, cache_content)
            return result

        return inner

    return _cache
