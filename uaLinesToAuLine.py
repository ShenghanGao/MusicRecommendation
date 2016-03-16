import sys
import math

from pyspark import SparkContext

def preMapLine(line):
    sp = line.split("\t")
    user = sp[0]
    [artist, rating] = sp[1].split(",")
    return (artist, (user, rating))

def mapLine((artist, (user, rating))):
    return (artist, user + "," + rating)

def mapToCalAvg((artist, (user, rating))):
    return (artist, (float(rating), 1))

def reduceToSum(value1, value2):
    return (value1[0] + value2[0], value1[1] + value2[1])

def mapToAvg((artist, (sumValue, count))):
    return (artist, sumValue / count)

def reduceToLine(value1, value2):
    return value1 + "\t" + value2

def ua_lines_to_au_line(file_name, au_out="ua_lines_to_au_line.out", artistAvgOut="artist_avg.out"):
    sc = SparkContext()
    file = sc.textFile(file_name)

    # au = file.map(mapLine).reduceByKey(reduceToLine)
    preMapped = file.map(preMapLine).cache()

    au = preMapped.map(mapLine)\
                  .reduceByKey(reduceToLine)

    artistAvg = preMapped.map(mapToCalAvg)\
                         .reduceByKey(reduceToSum)\
                         .map(mapToAvg)

    # print "preMapped: \n", preMapped.collect()
    # print "artistAvg: \n", artistAvg.collect()

    au.map(lambda x: x[0] + " " + x[1]).coalesce(1).saveAsTextFile(au_out)
    artistAvg.map(lambda x: x[0] + " " + str(x[1])).coalesce(1).saveAsTextFile(artistAvgOut)

if __name__ == "__main__":
    argv = sys.argv
    if len(argv) == 2:
        ua_lines_to_au_line(argv[1])
    else:
        ua_lines_to_au_line(argv[1], argv[2])

