package test_cases.moviecount;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import mapreduce.utils.Mapper;

// User defined mapper class for moviecount
//Extending UnicastRemoteObject to get added to RMI registry
public class MovieCountMapper extends UnicastRemoteObject implements Mapper {
    
    public MovieCountMapper() throws RemoteException {
        super();
    }

    // takes each line(movie name) and maps it to 1 (word = 1)
    public HashMap<String,List<String>> map(String docId,String input) throws RemoteException
    {
        HashMap<String,List<String>> map = new HashMap<>();
        String[] lines = input.split(System.lineSeparator());
        for(int line =0 ; line<lines.length;line++)
        {
                        if(lines[line].length()>0 && !lines[line].startsWith("###")) {   
                map.computeIfAbsent(lines[line], k -> new ArrayList<String>());
                map.get(lines[line]).add("1"); 
            }
        }
        return map;

    }
}