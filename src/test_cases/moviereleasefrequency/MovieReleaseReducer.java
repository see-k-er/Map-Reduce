package test_cases.moviereleasefrequency;


import mapreduce.utils.Reducer;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

// User defined mapper class for moviereleasefrequency
//Extending UnicastRemoteObject to get added to RMI registry
public class MovieReleaseReducer extends UnicastRemoteObject implements Reducer{

    public MovieReleaseReducer() throws RemoteException {
        super();
    }

    // user defined reduce method
    public List<String> reduce(String strin, List<String> list) throws RemoteException {
        ArrayList<String> result = new ArrayList<String>();
        int count = 0;
        for(String str : list)
        {
            try {
                count += 1;
            }
            catch (Exception e){}
        }
        String res = Integer.toString(count);
        result.add(res);
        return result;
    }
}
