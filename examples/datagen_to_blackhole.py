from pyflink.table import DataTypes, EnvironmentSettings, StreamTableEnvironment
from pyflink.table.udf import udf

env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
t_env = StreamTableEnvironment.create(environment_settings=env_settings)

# only needed in Flink 1.11.x
t_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", '80m')

concat = udf(lambda i, j: i + j, [DataTypes.STRING(), DataTypes.STRING()], DataTypes.STRING())

t_env.execute_sql("""
        CREATE TABLE my_source (
          a VARCHAR,
          b VARCHAR
        ) WITH (
          'connector' = 'datagen',
          'rows-per-second' = '1'
        )
    """)

t_env.execute_sql("""
        CREATE TABLE my_sink (
          `sum` VARCHAR
        ) WITH (
          'connector' = 'blackhole'
        )
    """)

t_env.register_function("concat", concat)

result = t_env.from_path('my_source') \
    .select("concat(a, b)") \
    .execute_insert('my_sink')

# wait the job to finish
# this is only needed when executed in local mode, e.g. executed in IDE or with command `python datagen_to_blackhole.py`
# result.get_job_client().get_job_execution_result().result()
