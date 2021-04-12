from pyflink.table import DataTypes
from pyflink.table.udf import udf


@udf(result_type=DataTypes.STRING())
def sub_string(s: str, begin: int, end: int):
    return s[begin:end]
