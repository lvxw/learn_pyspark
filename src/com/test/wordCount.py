from pyspark import SparkContext, SparkConf


def run():
    conf = SparkConf().setAppName("helloWorld").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.textFile('G:/项目/sync_remote_git_repository/learn_pyspark/tmp/logs/words')

    re = rdd.flatMap(lambda x: x.split(' '))\
        .map(lambda x: (x, 1))\
        .reduceByKey(lambda x, y: x+y)\
        .sortBy(lambda x: x[1], False)

    re.foreach(lambda x: print(x))


if __name__ == "__main__":
    print(__name__)
    # run()