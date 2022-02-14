from pyspark import SparkContext
import re

sc = SparkContext()
movie = sc.textFile("../../data/movie rating.txt")
inter = movie.collect()[:-1] #to remove end of line from input
movie_rat = sc.parallelize(inter) #conversion to rdd dataframe from list

#Splitting the text based on the lines
lines = movie_rat.flatMap(lambda txt: txt.lower().split("/n"))

#splitting the lines based on ":"
rate = lines.flatMap(lambda x: [list(x.split(":"))])

#taking sum of the ratings for each movie
rate_cnt = rate.map(lambda movie: (movie[0], int(movie[1])))
rate_rdd = rate_cnt.reduceByKey(lambda a, b: a+b).sortByKey()
rate_arr = rate_rdd.collect()

# write to file
f = open("../../spark_output/movie_rating_output.txt", "w")
for r in rate_arr:
    f.write("{:} : [{:}]\n".format(r[0], str(r[1])))
f.close()
