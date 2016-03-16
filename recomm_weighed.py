import sys

from pyspark import SparkContext

global userMap
global ratingMap

def mapUserLine(line):
    sp = line.split("\t")
    user = sp[0]
    [artist, rating] = sp[1].split(",")
    return (artist, float(rating))

def mapRatingLine(line):
    sp = line.split(" ")
    return (sp[0], float(sp[1]))

def mapCoMtxLine(line):
    sp = line.split(" ")
    return (sp[0], sp[1])

def mapCoMtxLineToWeighted(line):
    [artist, vector] = line.split(" ")
    targetAndWeights = vector.split("\t")

    sumOfProduct = 0
    sumOfAbsCoV = 0
    for targetAndWeight in targetAndWeights:
        [target, coV] = targetAndWeight.split(",")
        if target in userMap:
            sim = float(coV)
            userVal = userMap[target]
            avgVal = ratingMap[target]
            sumOfProduct += sim * (userVal - avgVal)
            sumOfAbsCoV += abs(sim)
    if sumOfAbsCoV == 0:
        return(artist, 0)
    else:
        predictedRating = sumOfProduct / sumOfAbsCoV + ratingMap[artist]
        return (artist, predictedRating)

def co_matrix(user_vec, artist_avg_rating, co_mtx, output="recomm.out"):
    sc = SparkContext()
    user_vector = sc.textFile(user_vec)
    art_avg_rt = sc.textFile(artist_avg_rating)
    co_matrix = sc.textFile(co_mtx)

    user = user_vector.map(mapUserLine)

    global userMap
    userMap = user.collectAsMap()

    artAvgRt = art_avg_rt.map(mapRatingLine)

    global ratingMap
    ratingMap = artAvgRt.collectAsMap()
    # print "ratingMap: \n", ratingMap

    recomm = co_matrix.map(mapCoMtxLineToWeighted)\
                      .filter(lambda x: x[0] not in userMap)\
                      .takeOrdered(20, key=lambda x: -x[1])

    # print "userMap: \n", userMap
    # print "recomm: \n", recomm
    sc.parallelize(recomm).map(lambda x: x[0] + " " + str(x[1])).coalesce(1).saveAsTextFile(output)
    

if __name__ == "__main__":
    argv = sys.argv
    argc = len(argv)
    if argc == 1:
        co_matrix("user1.txt", "10_co.txt")
    elif argc == 4:
        co_matrix(argv[1], argv[2], argv[3])
    else:
        co_matrix(argv[1], argv[2], argv[3], argv[4])

