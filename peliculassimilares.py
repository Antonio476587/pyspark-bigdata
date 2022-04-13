from math import sqrt
import sys
from pyspark import SparkConf, SparkContext

def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item", encoding="ascii", errors="ignore") as f:
        for line in f:
            fileLines = line.split("|")
            movieNames[int(fileLines[0])] = fileLines[1]
    return movieNames

def makePairs(userRatings):
    ratings = userRatings[1]
    (movie1, ratings1) = ratings[0]
    (movie2, ratings2) = ratings[1]
    return ((movie1, movie2), (ratings1, ratings2))

def filterDuplicates(userRatings):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2

def computeConsineSimilarity(ratingsPairs):
    numPairs =  0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingsPairs:
        sum_xx = ratingX * ratingX
        sum_yy = ratingY * ratingY
        sum_xy = ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)
    
    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))
    
    return (score, numPairs)

conf = SparkConf().setMaster("local[*]").setAppName("MovieSimilarities")
sc = SparkContext(conf = conf)

print("\nUploading movies names")
nameDict = loadMovieNames()

data = sc.textFile("file:///home/seo/Documents/Programming/cursospark/ml-100k/u.data")

ratings = data.map(lambda l: l.split()).map(lambda a: (int(a[0]), (int(a[1]), float(a[2]))))

joinedRatings = ratings.join(ratings)

uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

moviePairs = uniqueJoinedRatings.map(makePairs)

moviePairsRatings = moviePairs.groupByKey()

moviePairsSimilarities = moviePairsRatings.mapValues(computeConsineSimilarity).cache()

if (len(sys.argv) > 1):
    
    scoreThreshold = 0.97
    coOcurrenceThreshold = 50

    movieID = int(sys.argv[1])

    filteredResults = moviePairsSimilarities.filter(lambda pairSim: \
        (pairSim[0][0] == movieID or pairSim[0][1] == movieID) \
        and pairSim[1][0] > scoreThreshold and pairSim[1][1] > coOcurrenceThreshold)

    results = filteredResults.map(lambda pairSim: (pairSim[1], pairSim[0])).sortByKey().take(10)

    print("Top 10 similars movies for ", nameDict[movieID])

    for result in results:
        (sim, pair) = result
        similarMovieID = pair[0]
        if (similarMovieID == movieID):
            similarMovieID = pair[1]
        print(nameDict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstreng: " + str(sim[1]))