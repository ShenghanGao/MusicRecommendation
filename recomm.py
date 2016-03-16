import sys

from pyspark import SparkContext

global userMap

def mapUserLine(line):
    sp = line.split("\t")
    user = sp[0]
    [artist, rating] = sp[1].split(",")
    return (artist, float(rating))

def mapCoMtxLine(line):
    sp = line.split(" ")
    return (sp[0], sp[1])

def flatMapCoMtxLine((artistNo, vector)):
    li = []
    sps = vector.split("\t")
    for sp in sps:
        [artist, coV] = sp.split(",")
        userVal = userMap[artistNo]
        li.append((artist, userVal * float(coV)))
    return li

def reduceToRecommValue(value1, value2):
    return value1 + value2

def co_matrix(user_mtx, co_mtx, output="recomm.out"):
    sc = SparkContext("local[8]", "Recomm")
    user_vectors = sc.textFile(user_mtx)
    co_matrix = sc.textFile(co_mtx)

    user = user_vectors.map(mapUserLine)
    userKeys = user.keys().collect()

    global userMap
    userMap = user.collectAsMap()

    recomm = co_matrix.map(mapCoMtxLine)\
                      .filter(lambda x: x[0] in userKeys)\
                      .flatMap(flatMapCoMtxLine)\
                      .reduceByKey(reduceToRecommValue)\
                      .takeOrdered(1, key=lambda x: -x[1])
                      #.sortBy(lambda x: x[1], False)\
                      #.take(1)

    print "recomm:\n", recomm

    sc.parallelize(recomm).map(lambda x: x[0] + " " + str(x[1])).coalesce(1).saveAsTextFile(output)
    
    # co_pearson.map(lambda x: x[0] + " " + x[1]).coalesce(1).saveAsTextFile(output)

if __name__ == "__main__":
    argv = sys.argv
    argc = len(argv)
    if argc == 1:
        co_matrix("user1.txt", "10_co.txt")
    elif argc == 3:
        co_matrix(argv[1], argv[2])
    else:
        co_matrix(argv[1], argv[2], argv[3])

