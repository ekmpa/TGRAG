"""Import JSON modules with drop-in compatible API,
trying modules with faster JSON parsers first: orjson, ujson, json
Cf. https://github.com/commoncrawl/cc-pyspark/issues/41
"""

try:
    pass
except ImportError:
    try:
        pass
    except ImportError:
        pass
