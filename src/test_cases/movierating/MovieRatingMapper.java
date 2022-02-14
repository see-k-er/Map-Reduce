package test_cases.movierating;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

import mapreduce.utils.Mapper;

// User defined mapper class for movierating
//Extending UnicastRemoteObject to get added to RMI registry
public class MovieRatingMapper extends UnicastRemoteObject implements Mapper {

    public MovieRatingMapper() throws RemoteException {
        super();
    }

    // maps each movie name to its rating
    public HashMap<String,List<String>> map(String start_line,String input) throws RemoteException
    {
        HashMap<String,List<String>> map = new HashMap<>();
        String[] movienames = input.toLowerCase(Locale.ROOT).split("\\n");
        for(String str : movienames)
        {
            if(!str.equals("null") && str.contains(":")) {
                String[]  key_value = str.split(":");
                map.computeIfAbsent(key_value[0], k -> new ArrayList<String>());
                map.get(key_value[0]).add(key_value[1]);
            }
        }
        return map;

    }


}