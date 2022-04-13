import sys
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS, Rating

def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item", encoding="ascii", errors="ignore") as f:
        for line in f:
            fileLines = line.split("|")
            movieNames[int(fileLines[0])] = fileLines[1]
    return movieNames

conf = SparkConf().setMaster("local").setAppName("BebecitaEnAmazon")
sc = SparkContext(conf = conf)
sc.setCheckpointDir("checkpoint")

nameDict = loadMovieNames()
print("\nLoading movie names...")

data = sc.textFile("file:///home/seo/Documents/Programming/cursospark/ml-100k/u.data")

ratings = data.map(lambda l: l.split()).map(lambda a: Rating(int(a[0]), int(a[1]), float(a[2]))).cache()

print("\nTraining recomendation model")

rank = 10

numIterations = 150
model = ALS.train(ratings, rank, numIterations)

userID = int(sys.argv[1])

print("\nRatings for UserID " + str(userID) + ":")
userRatings = ratings.filter(lambda l: l[0] == userID)

for rating in userRatings.collect():
    print(nameDict[int(rating[1])] + ": " + str(rating[2]))

print("\nTop 10 recommendations:")
recommendations = model.recommendProducts(userID, 10)
for recommendation in recommendations:
    print(nameDict[int(recommendation[1])] + " score " + str(recommendation[2]))