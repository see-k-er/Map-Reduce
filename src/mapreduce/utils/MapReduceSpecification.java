package mapreduce.utils;

public class MapReduceSpecification {
    //This represents the number of mapper and reducer processes
    public int numOfProcesses;
    //Represents the input file location
    public String inputLocation;
    //Represents the output file location
    public String outputLocation;
    //Function which will execute the mapping processes(mapreduce.utils.Mapper)
    public Mapper mapper;
    //Function which will execute the reducer processes(mapreduce.utils.Reducer)
    public Reducer reducer;
    //Key to the mapper rmi object
    public String mapperRmiObj;
    //Key to the reducer rmi object
    public String reducerRmiObj;
    //Time limit for worker process(ms)
    public int timeout = 30000;
}
