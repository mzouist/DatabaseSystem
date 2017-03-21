
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RunSQL {

    private static int nthreads = 0;
    private static String driver = null;
    private static String connUrl = null;
    private static String connUser = null;
    private static String connPwd = null;
    private static List<ClusterInfo> clusterList = new ArrayList<ClusterInfo>();
    private static String from = null;
    private static String status = null;

    public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException {
        StringBuilder line = new StringBuilder();
        List<String> tokens = new ArrayList<String>();
        String lines;
        RunSQL run = new RunSQL();
        run.updateCatalog(args[0]);
        
        BufferedReader br = new BufferedReader(new FileReader(args[1]));
        while ((lines = br.readLine()) != null) {
            line.append(lines + " ");
        }
        if (line.lastIndexOf(";") > -1) {
            line.deleteCharAt(line.lastIndexOf(";"));
        }
        
        StringTokenizer st = new StringTokenizer(line.toString(), " ");
        while (st.hasMoreTokens()) {
            tokens.add(st.nextToken());
        }
        for (int i = 0; i < tokens.size(); i++) {                 // SELECTED ATTRIBUTES
            if (tokens.get(i).equalsIgnoreCase("from")) {
                if(tokens.get(i+1) != null)
                    from = tokens.get(i + 1);
            }
        }
        //System.out.println(from);
        
        run.getClusterNodes();
        Thread[] threads = new Thread[nthreads];
        
        //System.out.println(clusterList.size());
        
        for (int i = 0; i < nthreads; i++) {
            String driver = clusterList.get(i).getNodedriver();
            String connUrl = clusterList.get(i).getNodeurl();
            String connUser = clusterList.get(i).getNodeuser();
            String connPwd = clusterList.get(i).getNodepasswd();
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    Connection conn;
                    ResultSet resultSet;
                    String lines;
                    StringBuilder line = new StringBuilder();
                    String path = args[1];
                    try {
                        Class.forName(driver).newInstance();
                        conn = DriverManager.getConnection(connUrl + "?verifyServerCertificate=false&useSSL=true", connUser, connPwd);
                        //System.out.println(connUrl);
                        BufferedReader br = new BufferedReader(new FileReader(path));
                        while ((lines = br.readLine()) != null) {
                            line.append(lines);
                        }
                        if (line.lastIndexOf(";") > -1) {
                            line.deleteCharAt(line.lastIndexOf(";"));
                        }
                        String query = line.toString();
                        PreparedStatement statement = conn
                                .prepareStatement(query);
                        ResultSet rs = statement.executeQuery();
                        ResultSetMetaData rsmd = rs.getMetaData();
                        int columnsNumber = rsmd.getColumnCount();
                        while (rs.next()) {
                            StringBuilder valueCol = new StringBuilder();
                            for (int i = 1; i <= columnsNumber; i++) {
                                String columnValue = rs.getString(i).replaceAll("\\s+", " ");
                                valueCol.append(columnValue + " | ");
                            }
                            valueCol.setLength(valueCol.length() - 2);
                            System.out.println(valueCol);
                        }
                        status = "success";
                    } catch (IOException | ClassNotFoundException | IllegalAccessException | InstantiationException | SQLException e) {
                        System.err.println(e.getMessage());
                        status = "failed";
                    } 
                }
            });
            threads[i].start();
        }
        for (int i = 0; i < nthreads; i++) {
            threads[i].join();
        }
        System.out.println("[" + connUrl + "]: " + args[1] + " " + status + ".");
    }

    public void getClusterNodes() {
        Connection conn;
        ResultSet resultSet;
        int i = 0;
        String nodeDriver, nodeUrl, nodeUser, nodePasswd;
        try {
            Class.forName(driver).newInstance();
            //System.out.println(driver);
            conn = DriverManager.getConnection(connUrl + "?verifyServerCertificate=false&useSSL=true", connUser, connPwd);
            Statement stmt;
            String query = "SELECT * FROM DTABLES WHERE TNAME = '" + from +"'";
            //System.out.println(query);
            PreparedStatement statement = conn
                    .prepareStatement(query);
            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                ClusterInfo cluster = new ClusterInfo();
                nodeDriver = resultSet.getString("NODEDRIVER");
                nodeDriver = nodeDriver.replaceAll("\\s+", "");
                cluster.setNodedriver(nodeDriver);
                nodeUrl = resultSet.getString("NODEURL");
                //System.out.println(nodeUrl);
                nodeUrl = nodeUrl.replaceAll("\\s+", "");
                cluster.setNodeurl(nodeUrl);
                nodeUser = resultSet.getString("NODEUSER");
                nodeUser = nodeUser.replaceAll("\\s+", "");
                cluster.setNodeuser(nodeUser);
                nodePasswd = resultSet.getString("NODEPASSWD");
                nodePasswd = nodePasswd.replaceAll("\\s+", "");
                cluster.setNodepasswd(nodePasswd);
                i++;
                clusterList.add(cluster);
            }
            nthreads = i;
        } catch (Exception e) {
            //System.out.println(e.getMessage());
        }
    }

    public void updateCatalog(String filePath) throws FileNotFoundException, IOException {
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        Connection conn = null;
        String line = null, driver = null, hostname = null, username = null, passwd = null, tname = null;
        String catalogDriver = null, catalogHostname = null, catalogUsername = null, catalogPasswd = null;
        String status;
        while ((line = br.readLine()) != null) {
            if (line.startsWith("catalog")) {
                status = null;
                line = line.substring(line.lastIndexOf('=') + 1).trim();
                catalogDriver = line;
                this.driver = catalogDriver;
                line = br.readLine();
                line = line.substring(line.lastIndexOf('=') + 1).trim();
                catalogHostname = line;
                this.connUrl = catalogHostname;

                line = br.readLine();
                line = line.substring(line.lastIndexOf('=') + 1).trim();
                catalogUsername = line;
                this.connUser = catalogUsername;
                line = br.readLine();
                line = line.substring(line.lastIndexOf('=') + 1).trim();
                catalogPasswd = line;
                this.connPwd = catalogPasswd;
                
            }
        }
    }

}
