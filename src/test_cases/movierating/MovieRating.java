package test_cases.movierating;

import mapreduce.utils.MapReduce;
import mapreduce.utils.MapReduceSpecification;

import java.io.File;
import java.rmi.RemoteException;

// The main class that starts the MapReduce operation for movierating
public class MovieRating {

    public static void main(String[] args)
    {
        try {
            // declaring all input parameters
            MapReduceSpecification mp = new MapReduceSpecification();
            mp.numOfProcesses = 4;
            mp.inputLocation = System.getProperty("user.dir") + "/data/movie rating.txt";
            mp.outputLocation = System.getProperty("user.dir") + "/test_cases_output/movierating";
            File dir = new File(mp.outputLocation);
            dir.mkdir();
            mp.mapperRmiObj = "MovieRatingMapper";
            mp.reducerRmiObj = "MovieRatingReducer";
            mp.mapper = new MovieRatingMapper();
            mp.reducer = new MovieRatingReducer();
            MapReduce obj = new MapReduce();
            // call map reduce library
            obj.mapReduce(mp);
            System.exit(0); // required since mapper and reducer extend UnicastRemoteObject
        }
        catch(RemoteException ex) {
            ex.printStackTrace();
        }
    }

}
