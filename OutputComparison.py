import filecmp
from typing import DefaultDict

print("\nOUTPUT COMPARISON RESULTS\n")
#Test Case 1 - Movie Count
d1 = "doc/MovieCountMapperoutput.txt"
d2 = "spark_output/movie_count_output.txt"

f1, f2 = open(d1, 'r'), open(d2, 'r')

File1, File2 = open(d1,"r"), open(d2,"r")
Dict1, Dict2 = File1.readlines(), File2.readlines()

print("****Movie Count Result****")

Finaldict1 =[]
for line in Dict1:
    Finaldict1.append(line.lower())

Finaldict2 =[]
for line in Dict2:
    Finaldict2.append(line.lower())

sortedd1, sortedd2 = sorted(Finaldict1), sorted(Finaldict2)

main_list = list(set(sortedd1) - set(sortedd2)) #taking difference of the outputs from the two files
if len(main_list)<1   :
    print("MapReduce library and pyspark output have matched.\n")
else:
    print("Movie count output not matched.\n")


#Test Case 2 - Movie Rating
d1 = "doc/MovieRatingMapperoutput.txt"
d2 = "spark_output/movie_rating_output.txt"

f1, f2 = open(d1, 'r'), open(d2, 'r')

File1, File2 = open(d1,"r"), open(d2,"r")
Dict1, Dict2 = File1.readlines(), File2.readlines()

print("****Movie Rating Result****")

Finaldict1 =[]
for line in Dict1:
    Finaldict1.append(line.lower())

Finaldict2 =[]
for line in Dict2:
    Finaldict2.append(line.lower())

sortedd1, sortedd2 = sorted(Finaldict1), sorted(Finaldict2)

main_list = list(set(sortedd1) - set(sortedd2)) #taking difference of the outputs from the two files
if len(main_list)<1 :
    print("MapReduce library and pyspark output have matched.\n")
else:
    print("Movie rating output not matched.\n")


#Test Case 3 - Movie Release Frequency
d1 = "doc/MovieReleaseMapperoutput.txt"
d2 = "spark_output/movie_release_output.txt"

f1, f2 = open(d1, 'r'), open(d2, 'r')

File1, File2 = open(d1,"r"), open(d2,"r")
Dict1, Dict2 = File1.readlines(), File2.readlines()

print("****Movie Release Freqeuncy Result****")

Finaldict1 =[]
for line in Dict1:
    Finaldict1.append(line.lower())

Finaldict2 =[]
for line in Dict2:
    Finaldict2.append(line.lower())

sortedd1, sortedd2 = sorted(Finaldict1), sorted(Finaldict2)

main_list = list(set(sortedd1) - set(sortedd2)) #taking difference of the outputs from the two files
if len(main_list)<1 :
    print("MapReduce library and pyspark output have matched.\n")
else:
    print("Movie release frequency output not matched.\n")
