package test_cases.moviereleasefrequency;

import mapreduce.utils.MapReduce;
import mapreduce.utils.MapReduceSpecification;

import java.io.File;

// The main class that starts the MapReduce operation for moviereleasefrequency

public class MovieReleaseFrequency {

    public  static void main(String[] args) {
        try {
            // declaring all input parameters
            MapReduceSpecification mrs = new MapReduceSpecification();
            mrs.numOfProcesses = 4;
            mrs.inputLocation = System.getProperty("user.dir") + "/data/movie release frequency.txt";
            mrs.outputLocation = System.getProperty("user.dir") + "/test_cases_output/moviereleasefrequency";
            File dir = new File(mrs.outputLocation);
            dir.mkdir();
            mrs.mapperRmiObj = "MovieReleaseMapper";
            mrs.reducerRmiObj = "MovieReleaseReducer";
            mrs.mapper = new MovieReleaseMapper();
            mrs.reducer = new MovieReleaseReducer();
            MapReduce obj = new MapReduce();
            // call map reduce library
            obj.mapReduce(mrs);
            System.exit(0);  // required since mapper and reducer extend UnicastRemoteObject
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }
}
