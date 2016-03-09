import sys
import math

from pyspark import SparkContext

def mapLine(line):
    sp = line.split("\t")
    user = sp[0]
    [artist, rating] = sp[1].split(",")
    return (artist, user + "," + rating)

def reduceToLine(value1, value2):
    return value1 + "\t" + value2

def ua_lines_to_au_line(file_name, output="ua_lines_to_au_line.out"):
    sc = SparkContext("local[8]", "uaLinesToAuLine")
    file = sc.textFile(file_name)

    au = file.map(mapLine)\
            .reduceByKey(reduceToLine)

    au.map(lambda x: x[0] + " " + x[1]).coalesce(1).saveAsTextFile(output)

if __name__ == "__main__":
    argv = sys.argv
    if len(argv) == 2:
        ua_lines_to_au_line(argv[1])
    else:
        ua_lines_to_au_line(argv[1], argv[2])

