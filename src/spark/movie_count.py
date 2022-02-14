from pyspark import SparkContext
import re

sc = SparkContext()
movie = sc.textFile("../../data/movie count.txt")
inter = movie.collect()[:-1] #to remove end of line from input
movie_count = sc.parallelize(inter) #conversion to rdd dataframe from list

#Splitting the text based on the lines
words = movie_count.flatMap(lambda line: line.lower().split("\n"))

# word count sum
counts_rdd = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).sortByKey()
counts_tuple = counts_rdd.collect()

# write to file
f = open("../../spark_output/movie_count_output.txt", "w")
for r in counts_tuple:
    f.write("{:} : [{:}]\n".format(r[0], r[1]))
f.close()
