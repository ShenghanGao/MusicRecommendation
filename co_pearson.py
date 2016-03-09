import sys
import math

from pyspark import SparkContext

def mapLine(line):
    m = {}
    sp = line.split(" ")
    artist = sp[0]
    userAndRatings = sp[1].split("\t")
    for split in userAndRatings:
        [user, rating] = split.split(",")
        m[user] = rating
    return (artist, m)

def mapToPearson(((artist1, userAndRatings1), (artist2, userAndRatings2))):
    intersectionKeySet = set(userAndRatings1.keys()) & set(userAndRatings2.keys())
    length = len(intersectionKeySet)
    if length == 0:
        return ((artist1, artist2), 0)

    avg1 = 0
    avg2 = 0
    for key in intersectionKeySet:
        avg1 += float(userAndRatings1[key])
        avg2 += float(userAndRatings2[key])
    avg1 = avg1 / length
    avg2 = avg2 / length

    numerator = 0
    s1 = 0
    s2 = 0
    for key in intersectionKeySet:
        numerator += (float(userAndRatings1[key]) - avg1) * (float(userAndRatings2[key]) - avg2)
        s1 += (float(userAndRatings1[key]) - avg1) ** 2
        s2 += (float(userAndRatings2[key]) - avg2) ** 2

    if s1 == 0 or s2 == 0:
        return ((artist1, artist2), 0)

    denominator = math.sqrt(s1 * s2)

    return ((artist1, artist2), numerator / denominator)

def mapFormatting(((artist1, artist2), coValue)):
    key = artist1
    value = artist2 + "," + str(coValue)
    return (key, value)

def reduceToCo(value1, value2):
    return value1 + "\t" + value2

def co_matrix(file_name, output="co_matrix.out"):
    sc = SparkContext("local[8]", "UserArtistMatrix")
    file = sc.textFile(file_name)

    artistCom = file.map(mapLine)
    co_pearson = artistCom.cartesian(artistCom)\
                    .filter(lambda x: x[0][0] <= x[1][0])\
                    .map(mapToPearson)\
                    .filter(lambda x: x[1] != 0)\
                    .map(mapFormatting)\
                    .reduceByKey(reduceToCo)
    co_pearson.map(lambda x: x[0] + " " + x[1]).coalesce(1).saveAsTextFile(output)

if __name__ == "__main__":
    argv = sys.argv
    if len(argv) == 2:
        co_matrix(argv[1])
    else:
        co_matrix(argv[1], argv[2])

