import sys

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
    sp = s.rstrip("\t").split("\t")
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

def normalize(inputFile, outputFile):
    user = ""
    oneLine = ""
    with open(inputFile, "r") as fs:
        sps = fs.readline().rstrip("\n").split("\t")
        user = sps[0] + "\t"
        oneLine += user + sps[1] + "\t"
        for line in fs:
            artistAndRating = line.rstrip("\n").split("\t")[1]
            oneLine += artistAndRating + "\t"
    res = decouplingNormalize(oneLine)

    li = res[1]

    with open(outputFile, "w") as fs:
        for (artist, rating) in li:
            fs.write(user + artist + "," + str(rating) + "\n")

normalize(sys.argv[1], sys.argv[2])

