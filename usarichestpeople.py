from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("UsaRichestPeople")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(";")
    position = fields[0]
    name = fields[1]
    money = fields[2]
    country = fields[5]
    return (position, name, money, country)

lines = sc.textFile("file:///home/seo/Documents/Programming/cursospark/richestpeople.csv")
rdd = lines.map(parseLine)
richestPeople = rdd.filter(lambda x: "United States" in x[3])
usaRichestPeople = richestPeople.map(lambda x: (x[0], x[1], x[2]))
results = usaRichestPeople.collect()

for result in results:
    print(result)