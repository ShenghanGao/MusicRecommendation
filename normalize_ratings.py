import sys
import math

from pyspark import SparkContext

def decouplingNormalize(s):
    def convertRating(ele):
        [artist, rating] = ele.split(",")
        rating = int(rating)
        if rating == 0 or rating == 255:
            rating = 1
        else:
            rating = (rating - 1) / 10 + 1
        return (artist, rating)

    freq = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, 8: 0, 9: 0, 10: 0}
    pref = {}
    sp = s.split("\t")
    user = sp[0]
    artistAndRatings = sp[1:]
    length = len(artistAndRatings)
    artistAndRatings = map(convertRating, artistAndRatings)
    for (artist, rating) in artistAndRatings:
        freq[rating] += 1
    for key in freq:
        freq[key] /= float(length)
    pref[1] = freq[1]
    for i in range(2, 11):
        pref[i] = pref[i - 1] + freq[i]
    for key in pref:
        pref[key] = pref[key] - freq[key] / 2
    return (user, map(lambda (artist, rating): (artist, pref[rating]), artistAndRatings))

"""
def mapToPrint((user, artistAndRatings)):
    s = user + "\t"
    for (artist, rating) in artistAndRatings:
        s += artist + "\t" + str(rating)
    return s
"""

def flatMapToLines((user, artistAndRatings)):
    li = []
    for (artist, rating) in artistAndRatings:
        li.append((user, artist + "," + str(rating)))
    return li

def normalize_ratings(file_name, output="normalized_ratings.out"):
    sc = SparkContext("local[8]", "UserArtistMatrix")
    file = sc.textFile(file_name)

    normalizedRatings = file.map(decouplingNormalize)\
                            .flatMap(flatMapToLines)

    normalizedRatings.map(lambda x: x[0] + "\t" + x[1]).coalesce(1).saveAsTextFile(output)

if __name__ == "__main__":
    argv = sys.argv
    if len(argv) == 2:
        normalize_ratings(argv[1])
    else:
        normalize_ratings(argv[1], argv[2])

