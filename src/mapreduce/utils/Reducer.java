package mapreduce.utils;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

//Interface of Reducer class which user will implement
public interface Reducer extends Remote {
    List<String> reduce(String key, List<String> values) throws RemoteException;
}
