from pyspark import SparkContext

global friendsMap

def flatMapList((personID, friendIDList)):
    li = []
    for friendID in friendIDList:
        li.append((personID, friendID))
    return li

def mapToFriendList((personID, friendID)):
    return (personID, set(friendsMap[friendID]))

def reduceFriendSet(secondDegreeFriendIDSet1, secondDegreeFriendIDSet2):
    return secondDegreeFriendIDSet1 | secondDegreeFriendIDSet2

def mapToResult((personID, secondDegreeFriendIDSet)):
    li = list(secondDegreeFriendIDSet - set(friendsMap[personID]))
    li.remove(personID)
    return (personID, li)

def findSecondDegreeFriends():
    sc = SparkContext("local[8]", "findSecondDegreeFriends")
    friends = sc.parallelize([(1, [2]), (2, [1, 3, 4]), (3, [2]), (4, [2, 5]), (5, [4])])

    global friendsMap
    friendsMap = friends.collectAsMap()

    tmp = friends.flatMap(flatMapList)\
                 .map(mapToFriendList)\
                 .reduceByKey(reduceFriendSet)\
                 .map(mapToResult)

    print tmp.collect()

if __name__ == "__main__":
    findSecondDegreeFriends()

