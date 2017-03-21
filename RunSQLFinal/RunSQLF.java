
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.InputMismatchException;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RunSQLF {

    public static String inputArgs[] = new String[3];
    private static int nthreads = 0;
    private static String driver = null;
    private static String connUrl = null;
    private static String connUser = null;
    private static String connPwd = null;
    private static String localdriver = null;
    private static String localconnUrl = null;
    private static String localconnUser = null;
    private static String localconnPwd = null;
    private static String partitionTable = null;
    private static String partitionMethod = null;
    private static boolean catalog, numnodes, node, localnode, partition, tablename;

    public static void main(String[] args) throws IOException {
        if(args.length != 2){
            System.err.println("PLEASE INPUT 2 ARGUMENTS.");
            System.exit(0);
        }
        for (int i = 0; i < args.length; i++) {
            //System.out.println(args[i]);
        }
        catalog = false;
        numnodes = false;
        node = false;
        localnode = false;
        partition = false;
        tablename = false;
        String[] inputArgs = new String[2];

        try {
            inputArgs[0] = args[0];
            inputArgs[1] = args[1];
        } catch (java.lang.ArrayIndexOutOfBoundsException e) {
            System.err.println("PROGRAM REQUIRED 2 ARGUMENTS.");
        }

        LoadConfigFile(inputArgs[0]);

        try {
            if (catalog && numnodes && node && !localnode && !partition && !tablename) {
                System.out.println("RUNDDL");
                Runddl.main(inputArgs); // DONE
            } else if (catalog && !numnodes && !node && !localnode && !partition && !tablename) {
                System.out.println("RUNSQL");
                RunSQL.main(inputArgs); // DONE
            } else if (catalog && !node && !localnode && partition && tablename) {
                System.out.println("LOADCSV");
                loadCSV.main(inputArgs); // DONE
            } else if (catalog && !numnodes && !node && localnode && !partition && !tablename) {
                System.out.println("RUNSQL2");
                RunSQL2.main(inputArgs); // DONE
            }
        } catch (InterruptedException | FileNotFoundException e) {
            System.err.println(e.getMessage());
        } 

    }

    private static void LoadConfigFile(String filePath) throws FileNotFoundException, IOException {
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        try {
            String line = null, nodeDriver = null, nodeHost = null, nodeUser = null, nodePasswd = null;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("catalog")) {
                    catalog = true;
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    driver = line;
                    line = br.readLine();
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    connUrl = line;
                    line = br.readLine();
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    connUser = line;
                    line = br.readLine();
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    connPwd = line;
                }
                if (line.startsWith("numnodes")) {
                    numnodes = true;
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    nthreads = Integer.parseInt(line);
                }
                if (line.startsWith("node")) {
                    node = true;
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    nodeDriver = line;
                    line = br.readLine();
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    nodeHost = line;
                    line = br.readLine();
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    nodeUser = line;
                    line = br.readLine();
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    nodePasswd = line;
                }
                if (line.startsWith("localnode")) {
                    localnode = true;
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    localdriver = line;
                    line = br.readLine();
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    localconnUrl = line;
                    //System.out.println(this.connUrl);
                    line = br.readLine();
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    localconnUser = line;
                    line = br.readLine();
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    localconnPwd = line;
                }
                if (line.startsWith("tablename")) {
                    tablename = true;
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    partitionTable = line;

                }
                if (line.startsWith("partition")) {
                    partition = true;
                    String type = line;
                    type = type.substring(type.indexOf(".") + 1);
                    type = type.substring(0, type.indexOf("="));
                }

            }
        } finally {
            br.close();
        }

    }

}
