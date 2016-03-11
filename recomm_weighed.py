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

def mapCoMtxLineToWeighted(line):
    [artist, vector] = line.split(" ")
    targetAndWeights = vector.split("\t")

    sumOfProduct = 0
    for targetAndWeight in targetAndWeights:
        [target, coV] = targetAndWeight.split(",")
        if target in userMap:
            userVal = userMap[target]
            sumOfProduct += userVal * float(coV)
    return (artist, sumOfProduct)

def co_matrix(user_mtx, co_mtx, output="recomm.out"):
    sc = SparkContext("local[8]", "Recomm")
    user_vectors = sc.textFile(user_mtx)
    co_matrix = sc.textFile(co_mtx)

    user = user_vectors.map(mapUserLine)
    userKeys = user.keys().collect()

    global userMap
    userMap = user.collectAsMap()

    recomm = co_matrix.map(mapCoMtxLineToWeighted)\
                      .takeOrdered(5, key=lambda x: -x[1])

    print "userMap: \n", userMap
    print "recomm: \n", recomm
    sc.parallelize(recomm).map(lambda x: x[0] + " " + str(x[1])).coalesce(1).saveAsTextFile(output)
    

if __name__ == "__main__":
    argv = sys.argv
    argc = len(argv)
    if argc == 1:
        co_matrix("user1.txt", "10_co.txt")
    elif argc == 3:
        co_matrix(argv[1], argv[2])
    else:
        co_matrix(argv[1], argv[2], argv[3])

