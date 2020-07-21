"""
Created on Mon Jul 20 12:19 BRT 2020
author: guilherme passos | twitter: @gpass0s
"""

import re

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row
from pyspark.sql.types import *
from datetime import datetime

APACHE_ACCESS_LOG_PATTERN = (
    """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] \"(\S+) """
    + """(\S+)\s*(\S+)?\s*\" (\d{3}) (\S+) \"(\http://.+)\" \"(\w+/\d.+)\""""
)


def parse_apache_log_line(logline):
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        raise Exception("Invalid logline: %s" % logline)
    return Row(
        date_time=datetime.strptime(match.group(4).split(" ")[0], "%d/%b/%Y:%H:%M:%S"),
        ip_address=match.group(1),
        method=match.group(5),
        endpoint=match.group(6),
        protocol=match.group(7),
        response_code=int(match.group(8)),
        content_size=long(match.group(9)),
        http_referer=match.group(10),
        user_agent=match.group(11),
        full_message=match.group(0),
    )


def convert_and_save(rdd):
    if len(rdd.take(1)) != 0:
        schema = StructType(
            [
                StructField("date_time", TimestampType(), True),
                StructField("ip_address", StringType(), True),
                StructField("method", StringType(), True),
                StructField("endpoint", StringType(), True),
                StructField("protocol", StringType(), True),
                StructField("response_code", IntegerType(), True),
                StructField("content_size", LongType(), True),
                StructField("http_referer", StringType(), True),
                StructField("user_agent", StringType(), True),
                StructField("full_message", StringType(), True),
            ]
        )

        schema = StructType(sorted(schema, key=lambda f: f.name))
        log_files_data_frame = sqlContext.createDataFrame(rdd, schema)
        log_files_data_frame.coalesce(1).write.format("csv").options(
            header="true"
        ).options(mode="append").save(
            "/home/guilhermepassos/logs/{}".format(
                datetime.now().strftime("%y-%m-%d %H%M%S%f")
            )
        )


sc = SparkContext("local[2]", "NetworkWordCount")
sqlContext = SQLContext(sc)
ssc = StreamingContext(sc, 2)

stream_of_logs = ssc.socketTextStream("localhost", 7777)

parsed_logs = stream_of_logs.map(parse_apache_log_line)
parsed_logs.foreachRDD(convert_and_save)
parsed_logs.pprint()
ssc.start()
ssc.awaitTermination()
