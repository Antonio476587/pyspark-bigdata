from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("LovestSuperhero")
sc = SparkContext(conf = conf)

heros = sc.textFile("file:///home/seo/Documents/Programming/cursospark/marvel_graph.txt")
herosNames = sc.textFile("file:///home/seo/Documents/Programming/cursospark/marvel_names.txt")

rddNames = herosNames.map(lambda x: x.split()).map(lambda x: (x[0], x[1].encode("utf8")))

heroAndFriends = heros.map(lambda x: x.split()).map(lambda x: (x[0], len(x) - 1))
heroTotalFriends = heroAndFriends.reduceByKey(lambda x, y: (x + y))
flipedHero = heroTotalFriends.map(lambda x: (x[1], x[0]))

mostPopular = flipedHero.max()

mostPopularName = rddNames.lookup(mostPopular[1])[0]

print(str(mostPopular[0]) + " friends of " + str(mostPopularName))