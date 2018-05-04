# -*- coding: UTF-8 -*-

import sys


def run(input_path, output_path):
    from pyspark import SparkContext, SparkConf
    conf = SparkConf()\
        .set("spark.hadoop.validateOutputSpecs", "false") \
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .setAppName("helloWorld")

    sc = SparkContext(conf=conf)
    rdd = sc.textFile(input_path)

    re = rdd.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y).sortBy(lambda x: x[1], False)

    re.saveAsTextFile(output_path)

    sc.stop()


# 脚本传递的参数
# D:\python-package E:/project_sync_repository/learn_pyspark/tmp/logs/words E:/project_sync_repository/learn_pyspark/tmp/out/result
if __name__ == "__main__":
    python_package = sys.argv[1]
    input_path = sys.argv[2]
    output_path = sys.argv[3]
    sys.path.append(python_package)
    run(input_path, output_path)