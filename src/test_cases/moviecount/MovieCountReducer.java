package test_cases.moviecount;

import mapreduce.utils.Reducer;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

//User defined reducer class for moviecount
//Extending UnicastRemoteObject to get added to RMI registry
public class MovieCountReducer extends UnicastRemoteObject implements Reducer {
    

    public MovieCountReducer() throws RemoteException {
        super();
    }

    // reduce method for moviecount
    public List<String> reduce(String strin, List<String> list) throws RemoteException
    {
        ArrayList<String> result = new ArrayList<String>();
        int count = 0;
        // add all the counts for each key to get the final count
        for(String str : list)
        {
            try {
                count += Integer.parseInt(str);
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }

        String res = Integer.toString(count);
        result.add(res);

        return result;

    }
}