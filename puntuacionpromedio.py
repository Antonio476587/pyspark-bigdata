from pyspark import SparkContext, SparkConf

def preloadAnimeNames():
    animeNames = {}
    with open("anime.csv") as f:
        for line in f:
            fileLines = line.split(",")
            animeNames[int(fileLines[0])] = fileLines[1]
    return animeNames

conf = SparkConf().setMaster("local[3]").setAppName("PuntuacionPromedio")
sc = SparkContext(conf = conf)

animesDict = sc.broadcast(preloadAnimeNames())

def parseLine(line):
    fields = line.split(",")
    anime = int(fields[1])
    rating = int(fields[2])
    return (anime, rating)

def changeNames(line):
    rate = int(line[0])
    if rate < 1:
        rate = 1
    if animesDict.value.get(line[1]) is not None:
        return (rate, animesDict.value[line[1]])
    else:
        return (rate, str(line[1]))

lines = sc.textFile("file:///home/seo/Documents/Programming/cursospark/rating.csv")
rdd = lines.map(parseLine)
totalByAnime = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averageByAnime = totalByAnime.mapValues(lambda x: x[0] / x[1])
flipAnime = averageByAnime.map(lambda x: (int(x[1]), x[0]))
sortedAnimeWithName = flipAnime.map(changeNames).sortByKey()

sortedAnimeWithName.saveAsTextFile("Animes")

results = sortedAnimeWithName.collect()

for result in results:
    print(result)