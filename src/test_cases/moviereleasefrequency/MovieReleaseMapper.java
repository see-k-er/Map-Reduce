package test_cases.moviereleasefrequency;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import mapreduce.utils.Mapper;

//User defined mapper class for moviereleasefequency
//Extending UnicastRemoteObject to get added to RMI registry
public class MovieReleaseMapper extends UnicastRemoteObject implements Mapper {

    public MovieReleaseMapper() throws RemoteException {
        super();
    }

    // maps year to movie name
    public HashMap<String, List<String>> map(String docId, String input) throws RemoteException{
        HashMap<String,List<String>> map = new HashMap<>();
       
        String[] words = input.toLowerCase().split("\\n");

        for(String str : words)
        {
            if(!str.equals("null") && !str.startsWith("###")) {
                String[]  key_value = str.split(":");
                map.computeIfAbsent(key_value[0], k -> new ArrayList<String>());
                if(key_value.length==2 && !map.get(key_value[0]).contains(key_value[1]))
                    map.get(key_value[0]).add(key_value[1]); // year -> moviename
            }
        }
        return map;
    }
}
