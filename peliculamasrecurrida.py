from pyspark import SparkConf, SparkContext

def loadPelisName():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            movieFields = line.split("|")
            movieNames[int(movieFields[0])] = movieFields[1]
    return movieNames


conf = SparkConf().setMaster("local").setAppName("PelisMasRecurridas")
sc = SparkContext(conf = conf)

nameDict = sc.broadcast(loadPelisName())

pelisFile = sc.textFile("file:///home/seo/Documents/Programming/cursospark/ml-100k/u.data")
pelis = pelisFile.map(lambda x: (int(x.split()[1]), 1))
pelisCount = pelis.reduceByKey(lambda x, y: (x + y))

flipedPelis = pelisCount.map(lambda x: (x[1], x[0]))

flipedPelisWithName = flipedPelis.map(lambda x: (x[0], nameDict.value[x[1]]))

sortedPelis = flipedPelisWithName.sortByKey()

sortedPelis.saveAsTextFile("Pelis")

results = sortedPelis.collect()

for result in results:
    print(result[1] + "---" + str(result[0]))