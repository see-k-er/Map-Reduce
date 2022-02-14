package mapreduce.utils;

import java.io.*;
import java.math.BigInteger;
import java.rmi.RemoteException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


//This class defines the implementation of a mapper worker node
public class MapperWorker {
    //This function reads from input file and writes to intermediate files
    public void mapperExecute(Mapper mapperObj, String ipFileLoc, int startLine, int endLine, int numOfProcesses, int currProcessNum) {
        StringBuilder input = new StringBuilder();
        //This block reads from the input file
        try {
            FileInputStream filestream = new FileInputStream(ipFileLoc);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(filestream));
            for (int i = 0; i < startLine; ++i)
                bufferedReader.readLine();
            for (int i = startLine; i<=endLine; i++) {
                input.append(bufferedReader.readLine());
                input.append("\n");
            }
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        // This block creates writers to intermediate files for each mapper
        try {
            HashMap<String, List<String>> output = mapperObj.map(String.valueOf(startLine), input.toString());
            List<BufferedWriter> bufferedWriter = new ArrayList<>();
            try {
                // intermediate file name will be of the form mapperoutput_processNUmber.txt
                String tempMapperFileName = "mapperoutput_" + currProcessNum + ".txt";
                for (int i = 0; i < numOfProcesses; i++) {
                    File directory = new File("reduceroutput_" + i);
                    if (!directory.exists()) directory.mkdir();

                    File tempMapperFile = new File("reduceroutput_" + i + "/" + tempMapperFileName);
                    if (!tempMapperFile.exists()) {
                        tempMapperFile.createNewFile();
                    }
                    FileWriter fw = new FileWriter(tempMapperFile.getAbsoluteFile());
                    BufferedWriter bw = new BufferedWriter(fw);
                    bufferedWriter.add(bw);
                }
                output.forEach((key, value) -> {
                    //This block ensures that all keys with the same name are assigned to the same reducer.
                    //MD5 hashing algo was used
                    //The output was between 0 to N.
                    // No shuffling of keys required later.
                    MessageDigest md = null;
                    try {
                        try {
                            md = MessageDigest.getInstance("MD5");
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }
                        md.update(key.getBytes("UTF-8"));
                    } catch (UnsupportedEncodingException e1) {
                        e1.printStackTrace();
                    }
                    final int hash_value = Math.abs(new BigInteger(1, md.digest()).intValue()%numOfProcesses);

                    value.forEach(val -> {
                        try {
                            BufferedWriter bw = bufferedWriter.get(hash_value);
                            bw.write(key + ":" + val);
                            bw.write("\n");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                });
                for (BufferedWriter bw : bufferedWriter) {
                    bw.close();
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        catch(RemoteException ex) {
            ex.printStackTrace();
        }
    }
    //This gets invoked by master as a separate process
    public static void main(String[] args) throws Exception {
        String mapperKey = args[0];
        String ipFileLoc = args[1];
        String startLine = args[2];
        String endLine = args[3];
        String numOfProcesses = args[4];
        String process_num = args[5];
        int timeout = Integer.parseInt(args[6]);
        timeout = timeout > 0 ? timeout : 7000;
        while(args.length > 7 && Integer.parseInt(process_num) == Integer.parseInt(args[7])); // to simulate fault tolerance
        MapReduce mr = new MapReduce();
        Mapper mapperObj = null;
        try {
            mapperObj = mr.getMapperObject(mapperKey); // get user defined mapper object from RMI registry
        }
        catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        MapperWorker mw = new MapperWorker();
        mw.mapperExecute(mapperObj, ipFileLoc, Integer.parseInt(startLine), Integer.parseInt(endLine),
                Integer.parseInt(numOfProcesses), Integer.parseInt(process_num));

    }
}
