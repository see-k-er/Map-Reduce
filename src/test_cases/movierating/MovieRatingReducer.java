package test_cases.movierating;
import mapreduce.utils.Reducer;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

// User defined mapper class for movierating
//Extending UnicastRemoteObject to get added to RMI registry
public class MovieRatingReducer extends UnicastRemoteObject implements Reducer{

    public MovieRatingReducer() throws RemoteException {
        super();
    }

    // reduce method for movierating. Adds the ratings for each key
    public List<String> reduce(String strin,List<String> list) throws RemoteException {
        ArrayList<String> result = new ArrayList<String>();
        int count = 0;
        for(String str : list)
        {
            try {
                count += Integer.parseInt(str);
            }
            catch (Exception e){}
        }

        String res = Integer.toString(count);
        result.add(res);

        return result;

    }
}
