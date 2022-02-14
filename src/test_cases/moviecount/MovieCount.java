package test_cases.moviecount;

import mapreduce.utils.MapReduce;
import mapreduce.utils.MapReduceSpecification;

import java.io.File;

//The main class that starts the MapReduce operation for moviecount
 public class MovieCount {

    public static void main(String[] args)
    {
        try {
            // declaring all input parameters
            MapReduceSpecification mp = new MapReduceSpecification();
            mp.numOfProcesses = 4;
            mp.inputLocation = System.getProperty("user.dir") + "/data/movie count.txt";
            mp.outputLocation = System.getProperty("user.dir") + "/test_cases_output/moviecount";
            File dir = new File(mp.outputLocation);
            dir.mkdir();
            mp.mapperRmiObj = "MovieCountMapper";
            mp.reducerRmiObj = "MovieCountReducer";
            mp.mapper = new MovieCountMapper();
            mp.reducer = new MovieCountReducer();
            MapReduce obj = new MapReduce();
            // call map reduce library
            obj.mapReduce(mp);
            System.exit(0); // required since mapper and reducer extend UnicastRemoteObject
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
