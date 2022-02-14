# map-reduce-rangers

# PROJECT 1 - MAPREDUCE LIBRARY

## **Overview**
For this project, we have implemented MapReduce Library using Java. We have taken three test cases to show the working of the MapReduce function.
 The three test cases are:
 1. Movie Count - Given a data set with a list of movies watched by a user, the mapreduce library will return the number of people who have watched the movie.
 2. Movie Rating - Given a data set of movies with their ratings from different users, the mapreduce library will return the rating score out of 100 for each movie.
 3. Movie Release Frequency - For a data set of movies with their release dates, the mapreduce library will return the number of movies released in the respective year.

## **Code Execution**
To execute the three test cases, the shell script "_run-test-cases.sh_" should be run from the source directory. This compiles all the library code with the test cases and then runs them.
Execute ```./run-test-cases.sh``` or ```sh run-test-cases.sh```

The output of the MapReduce Library for the above three test cases, can be verified with the output from the PySpark implementation of the same. The PySpark implementation is present in the src/spark folder. Each python corresponds to a test case.
Execute ```spark-submit filename.py```

On running the bash script, OutputComparison.py is also executed. This compares the output of MapReduce Library and the PySpark implementation for each of the test case and tells if the results are matching or not, which can be seen as output in the terminal.
Note – The code is best implemented on a Mac system.

## **Output**
The MapReduce library will give N output files, based on the number of processes. The PySpark implementation will give one output file for a single test case execution.
The output for the test cases are present in docs/ directory. The output for the PySpark implementation is present in the “spark_output” folder in the root directory.
