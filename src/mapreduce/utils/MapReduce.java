package mapreduce.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

//This is the main class for MapReduce functionality which invokes map and reduce code.
public class MapReduce {

    String mapperRmiObj;
    String reducerRmiObj;

    public void mapReduce(MapReduceSpecification specs) {
        insertInRmiRegistry(specs.mapper, specs.mapperRmiObj);
        insertInRmiRegistry(specs.reducer, specs.reducerRmiObj);
        String inputLocation = specs.inputLocation;
        int totalLines = getTotalLines(inputLocation);
        List<Process> mapperProcesses = new ArrayList<>();
        List<Process> reducerProcesses = new ArrayList<>();
        int numberOfProcesses = specs.numOfProcesses;
        int divisionLength = totalLines/(specs.numOfProcesses);
        //Randomly mark one mapper and one reducer as faulty.
        int faultyMapper = selectFaultyWorker(numberOfProcesses);
        int faultyReducer = selectFaultyWorker(numberOfProcesses);

        try {
            System.out.println(specs.mapperRmiObj +" has started execution.");
            // start all mapper nodes
            for (int i=0; i<numberOfProcesses; i++) {
                int startLine = i*divisionLength;
                int endLine = startLine+divisionLength-1;
                if (endLine>=totalLines) {
                    endLine = totalLines-1;
                }
                //System.out.println(specs.mapperRmiObj + "-"+i+" working from line " + startLine + " to line " + endLine);
                ProcessBuilder processBuilderMapper = new ProcessBuilder("java",
                        "-cp", "./src", "mapreduce/utils/MapperWorker", specs.mapperRmiObj,
                        inputLocation, String.valueOf(startLine), String.valueOf(endLine),
                        String.valueOf(numberOfProcesses), String.valueOf(i), String.valueOf(specs.timeout), String.valueOf(faultyMapper));

                Process mprocess = processBuilderMapper.inheritIO().start();
                mapperProcesses.add(mprocess);
            }

            // wait for all mapper processes to complete
            for (int i=0; i<mapperProcesses.size(); i++) {
                boolean timeOut = false;
                
                if(!mapperProcesses.get(i).waitFor(specs.timeout, TimeUnit.MILLISECONDS)) {
                    mapperProcesses.get(i).destroy();
                    timeOut = true;
                }
                System.out.println(specs.mapperRmiObj + "-"+(i>=numberOfProcesses? faultyMapper : i)+(timeOut || mapperProcesses.get(i).exitValue() != 0 ? " id Straggler node failed! Retrying...":""));
                if(i<numberOfProcesses && (timeOut || mapperProcesses.get(i).exitValue() != 0)) {
                    // if there was a fault in any original worker, then restart that worker
                    int startLine = i*divisionLength;
                    int endLine = startLine+divisionLength-1;
                    if (endLine>=totalLines) {
                        endLine = totalLines-1;
                    }
                    ProcessBuilder processBuilderMapper = new ProcessBuilder("java",
                            "-cp", "./src", "mapreduce/utils/MapperWorker", specs.mapperRmiObj,
                            inputLocation, String.valueOf(startLine), String.valueOf(endLine),
                            String.valueOf(numberOfProcesses), String.valueOf(i), String.valueOf(specs.timeout));
                    Process mp = processBuilderMapper.inheritIO().start();
                    mapperProcesses.add(mp);
                }
            }

            System.out.println(specs.mapperRmiObj +" has finished executing successfully.");

            System.out.println(specs.reducerRmiObj +" has started execution.");
            // start all reducer processes
            for (int i=0; i<numberOfProcesses; i++) {
                String opFilePath = specs.outputLocation + "/division_" + i + ".txt";
                ProcessBuilder processBuilderReducer = new ProcessBuilder("java",
                        "-cp", "./src", "mapreduce/utils/ReducerWorker",
                        opFilePath, "reduceroutput_" + i, specs.reducerRmiObj, String.valueOf(specs.timeout), String.valueOf(faultyReducer));
                Process reducer = processBuilderReducer.inheritIO().start();
                reducerProcesses.add(reducer);
            }

            // wait for all reducer processes to complete
            for (int i=0; i<reducerProcesses.size(); i++) {
                boolean timeOut = false;
                if(!reducerProcesses.get(i).waitFor(specs.timeout, TimeUnit.MILLISECONDS)) {
                    reducerProcesses.get(i).destroy();
                    timeOut = true;
                }
                System.out.println(specs.reducerRmiObj + "-"+(i>=numberOfProcesses? faultyReducer : i)+(timeOut || reducerProcesses.get(i).exitValue() != 0 ? " id Straggler node failed! Retrying..." : " finished successfully."));
                if(i<numberOfProcesses && (timeOut || reducerProcesses.get(i).exitValue() != 0)) {
                    // if there was a fault in any original worker, then restart that worker
                    String opFilePath = specs.outputLocation + "/division_" + i + ".txt";
                    ProcessBuilder processBuilderReducer = new ProcessBuilder("java",
                            "-cp", "./src", "mapreduce/utils/ReducerWorker",
                            opFilePath, "reduceroutput_" + i, specs.reducerRmiObj, String.valueOf(specs.timeout));
                    Process reducer = processBuilderReducer.inheritIO().start();
                    reducerProcesses.add(reducer);
                }
            }
            System.out.println(specs.reducerRmiObj +" has finished executing successfully.");
            System.out.println("Map Reduce completed for "+specs.mapperRmiObj+"/"+specs.reducerRmiObj+"\n");
            for (int i=0; i<numberOfProcesses; i++) {
                // delete all the temporary files
                File tempDir = new File("reduceroutput_" + i);
                for (File file: tempDir.listFiles())
                    if (!file.isDirectory())
                        file.delete();
                tempDir.delete();

            }
            mergefiles(specs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void mergefiles(MapReduceSpecification specs)
    {
        Path path = null;
        try {

            path = Paths.get(System.getProperty("user.dir")+"/doc");
        
            //java.nio.file.Files;
            Files.createDirectories(path);
                
          } catch (Exception e) {
        
            System.err.println("Failed to create directory!" + e.getMessage());
        
          }


        File dir = new File(specs.outputLocation);
        PrintWriter pw =null;
        System.out.println("Combining contents of "+specs.reducerRmiObj+" outputs to one final.");
        // create object of PrintWriter for output file
        try{

         pw = new PrintWriter(System.getProperty("user.dir")+"/doc/"+specs.mapperRmiObj+"output.txt");
    }
    catch(Exception e)
    {
        e.printStackTrace();
    }
        // Get list of all the files in form of String Array
        String[] fileNames = dir.list();

        // loop for reading the contents of all the files
        // in the directory
        BufferedReader br = null;
        for (String fileName : fileNames) {
            
 
            // create instance of file from Name of
            // the file stored in string Array
            File f = new File(dir, fileName);            
            try {
                br = new BufferedReader(new FileReader(f));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            try{
            // Read from current file
            String line = br.readLine();
            while (line != null) {
                // write to the output file
                pw.println(line);
                line = br.readLine();
            }
            pw.flush();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        }
        System.out.println("Combining results from all files in directory " + dir.getName() + " completed");
    }
        
    

    //Generate a faulty node
    private int selectFaultyWorker(int numberOfProcesses) {
        int faultyNode =  (int) (Math.random() * numberOfProcesses);
        return faultyNode;
    }
    //counts total lines in file
    private int getTotalLines(String inputLocation) {
        int lineCount = 0 ;
        try (Stream<String> lines = Files.lines(Paths.get(inputLocation))) {
            lineCount =  (int) lines.count();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return lineCount;
    }

    // inserts mapper object in the RMI registry
    public void insertInRmiRegistry(Mapper obj, String objKey) {
        try {
            mapperRmiObj = objKey;
            // if the RMI registry doesn't already exist, then create it
            try {
                LocateRegistry.createRegistry(1099);
            }
            catch(Exception e) {
                e.printStackTrace();
            }

            // Bind to the registry
            Registry registry = LocateRegistry.getRegistry();
            registry.bind(objKey, obj);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    // inserts reducer object in the RMI registry
    public void insertInRmiRegistry(Reducer obj, String objKey) {
        try {
            reducerRmiObj = objKey;

            // Bind to the registry
            Registry registry = LocateRegistry.getRegistry();
            registry.bind(objKey, obj);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    // returns user defined mapper object from RMI registry
    public Mapper getMapperObject(String objKey) throws Exception {
        try {
            Mapper mapperObj = (Mapper) Naming.lookup("rmi://localhost/"+objKey);
            return mapperObj;
        }
        catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    // returns user defined reducer object from RMI registry
    public Reducer getReducerObject(String objKey) throws Exception {
        try {
            Reducer reducerObj = (Reducer) Naming.lookup("rmi://localhost/"+objKey);
            return reducerObj;
        }
        catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

}
