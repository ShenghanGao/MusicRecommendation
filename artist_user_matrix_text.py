import sys

from pyspark import SparkContext

def mapLine(line):
    line_split = line.split('\t')
    key = line_split[1]
    value = (line_split[0], line_split[2])
    return (key, value)

def flatMapArtist((artist, userAndRatings)):
    li = []
    for (user, rating) in userAndRatings:
        userAndRating = artist + "," + rating
        li.append((user, userAndRating))
    return li

def reduceByKeyToLine(value1, value2):
    return value1 + "\t" + value2

def artist_user_matrix(file_name, output="filtered_user_matrix.out"):
    sc = SparkContext("local[8]", "FilteredUserMatrix")
    file = sc.textFile(file_name)

    filteredUa = file.map(mapLine)\
                 .groupByKey()\
                 .filter(lambda x: len(x[1]) >= 2)\
                 .flatMap(flatMapArtist)\
                 .reduceByKey(reduceByKeyToLine)

    filteredUa.map(lambda x: x[0] + "\t" + x[1]).coalesce(1).saveAsTextFile(output)

if __name__ == "__main__":
    argv = sys.argv
    if len(argv) == 2:
        artist_user_matrix(argv[1])
    else:
        artist_user_matrix(argv[1], argv[2])

