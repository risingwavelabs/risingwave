import sys
from typing import Iterator, List, Optional, Tuple, Any
from decimal import Decimal

sys.path.append("src/udf/python")  # noqa

from risingwave.udf import udf, UdfServer



@udf(input_types=["INT", "VARCHAR"], result_type="INT")
def udf_sum(x: int, y: str) -> int:
    if y=='mfa+':
        return x
    else:
        return -x



if __name__ == "__main__":
    server = UdfServer(location="0.0.0.0:8815")
    server.add_function(udf_sum)
    server.serve()