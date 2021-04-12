from pyflink.common import Row
from pyflink.table import AggregateFunction, DataTypes
from pyflink.table.udf import udaf


class WeightedAvg(AggregateFunction):

    def create_accumulator(self):
        # Row(sum, count)
        return Row(0, 0)

    def get_value(self, accumulator: Row) -> float:
        if accumulator[1] == 0:
            return 0
        else:
            return accumulator[0] / accumulator[1]

    def accumulate(self, accumulator: Row, value, weight):
        accumulator[0] += value * weight
        accumulator[1] += weight

    def retract(self, accumulator: Row, value, weight):
        accumulator[0] -= value * weight
        accumulator[1] -= weight


weighted_avg = udaf(f=WeightedAvg(),
                    result_type=DataTypes.DOUBLE(),
                    accumulator_type=DataTypes.ROW([
                        DataTypes.FIELD("f0", DataTypes.BIGINT()),
                        DataTypes.FIELD("f1", DataTypes.BIGINT())]))
