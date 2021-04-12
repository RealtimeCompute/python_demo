from pyflink.table import DataTypes
from pyflink.table.udf import udtf


@udtf(result_types=[DataTypes.STRING(), DataTypes.STRING()])
def split(s: str):
    splits = s.split("|")
    yield splits[0], splits[1]
