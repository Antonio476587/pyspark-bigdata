import re
from pyspark import SparkConf, SparkContext

def normalizeWords(word):
    return re.compile(r"\W+", re.UNICODE).split(word.lower())

conf = SparkConf().setMaster("local").setAppName("MostUsedWords")
sc = SparkContext(conf = conf)

inputFile = sc.textFile("file:///home/seo/Documents/Programming/cursospark/book")
words = inputFile.flatMap(normalizeWords)

wordsCount = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordsSorted = wordsCount.map(lambda x: (x[1], x[0])).sortByKey()
results = wordsSorted.collect()

for count, word in results:
    cleanWord = word.encode("ascii", "ignore")
    if (cleanWord):
        print(cleanWord.decode() + ":\t" + str(count))