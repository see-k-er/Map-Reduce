from pyspark import SparkContext
import re

sc = SparkContext()
movie = sc.textFile("../../data/movie release frequency.txt")
inter = movie.collect()[:-1] #to remove end of line from input
movie_rel = sc.parallelize(inter) #conversion to rdd dataframe from list

#Splitting the text based on the lines
lines = movie_rel.flatMap(lambda txt: txt.lower().split("/n"))

#splitting the lines based on ":"
year = lines.flatMap(lambda x: [list(x.split(":"))[0]])

#taking count for each movie entry
yr_count = year.map(lambda word: (word, 1))
yr_rdd = yr_count.reduceByKey(lambda a, b: a+b).sortByKey()
yr_arr = yr_rdd.collect()

# write to file
f = open("../../spark_output/movie_release_output.txt", "w")
for r in yr_arr:
    f.write("{:} : [{:}]\n".format(r[0], str(r[1])))
f.close()
