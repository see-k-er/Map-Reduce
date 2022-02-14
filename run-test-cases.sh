if [[ "$OSTYPE" == "linux-gnu"* ]]; then
  find -name "*.java" > sources.txt
  javac @sources.txt
else
  javac src/**/*.java
fi

java -cp ./src test_cases/moviecount/MovieCount
java -cp ./src test_cases/movierating/MovieRating
java -cp ./src test_cases/moviereleasefrequency/MovieReleaseFrequency
python3 OutputComparison.py
