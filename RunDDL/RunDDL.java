
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class RunDDL {

    private static int nthreads = 0;
    private static String driver = null;
    private static String connUrl = null;
    private static String connUser = null;
    private static String connPwd = null;
    private static List<ClusterInfo> list = new ArrayList<ClusterInfo>();
    //jdbc:derby://localhost:1527/testing
    public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException {
       
        /* ************* MAIN PROGRAM **************/
        RunDDL dt = new RunDDL();
        ClusterInfo node = new ClusterInfo();

        
        //Read clustercfg.cfg and insert into dtable
        dt.readCfg(args[0]);
        Thread[] tList = new Thread[nthreads];
        
        String ddlFile = args[1];

        for (int i = 0; i < nthreads; i++) {
            node = list.get(i);
            //System.out.println(tList.length);
            //DO NOT DELETE
            tList[i] = new Thread(new ThreadConnection(node.getNodedriver(), node.getNodeurl(), node.getNodeuser(), node.getNodepasswd(), ddlFile));
            tList[i].start();
        }
        for (int ij = 0; ij < nthreads; ij++) {
            tList[ij].join();
        }
        
        

    }

    private void readCfg(String filePath) throws FileNotFoundException, IOException {
        int currentid = 1;
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        try {
            String line = null, driver = null, hostname = null, username = null, passwd = null;
            String cdriver = null, chostname = null, cusername = null, cpasswd = null;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("catalog")) {
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    cdriver = line;
                    this.driver = cdriver;
                    System.out.println(cdriver);
                    line = br.readLine();
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    chostname = line;
                    this.connUrl = chostname;
                    line = br.readLine();
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    cusername = line;
                    this.connUser = cusername;
                    line = br.readLine();
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    cpasswd = line;
                    this.connPwd = cpasswd;

                    this.ReadCatalog(cdriver, chostname, cusername, cpasswd);
                }
                if (line.startsWith("numnodes")) {
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    nthreads = Integer.parseInt(line);
                }
                if (line.startsWith("node")) {
                    ClusterInfo ci = new ClusterInfo();
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    driver = line;
                    ci.setNodedriver(driver);
                    line = br.readLine();
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    hostname = line;
                    ci.setNodeurl(hostname);
                    line = br.readLine();
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    username = line;
                    ci.setNodeuser(username);
                    line = br.readLine();
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    passwd = line;
                    ci.setNodepasswd(passwd);
                    ci.setNodeid(currentid);
                    list.add(ci);
                    this.insertNode(cdriver, chostname, cusername, cpasswd, driver, hostname, username, passwd, currentid);
                    currentid++;
                }

            }
        } finally {
            br.close();
        }

    }

    private void insertNode(String conDriver, String conUrl, String conUser, String conPwd,
        String driver, String url, String userName, String password, int id) {
        Connection conn = null;
        try {
            Class.forName(conDriver).newInstance();
            conn = DriverManager.getConnection(conUrl, conUser, conPwd);
            PreparedStatement pstmt;
            pstmt = conn.prepareStatement("INSERT INTO ICS421.DTABLES (NODEDRIVER, NODEURL, NODEUSER, NODEPASSWD, NODEID) VALUES (?, ?, ?, ?, ?)");
            pstmt.setString(1, driver);
            pstmt.setString(2, url);
            pstmt.setString(3, userName);
            pstmt.setString(4, password);
            pstmt.setInt(5, id);
            pstmt.executeUpdate();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void ReadCatalog(String inputDriver, String inputUrl, String inputUserName, String inputPassword) {
        //System.out.println("Connecting to Derby Database: " + inputUrl);
        Connection conn = null;
        String url = inputUrl;
        String driver = inputDriver;
        String userName = inputUserName;
        String password = inputPassword;
        String status = null;
        try {
            Class.forName(driver).newInstance();
            //conn = DriverManager.getConnection(url, userName, password);
            conn = DriverManager.getConnection(url + ";create=true", userName, password);
            //System.out.println("Connected to the database");

            String query = "DROP TABLE ICS421.DTABLES";
            Statement stmt = null;
            
            try {
                stmt = conn.createStatement();
                stmt.executeUpdate(query);
            } catch (SQLException e) {
                if (tableAlreadyExists(e)) {
                    return;
                }
            } finally {
                query = "CREATE TABLE ICS421.DTABLES(tname char(32), \n"
                        + "   nodedriver char(64), \n"
                        + "   nodeurl char(128), \n"
                        + "   nodeuser char(16), \n"
                        + "   nodepasswd char(16), \n"
                        + "   partmtd int, \n"
                        + "   nodeid int, \n"
                        + "   partcol char(32), \n"
                        + "   partparam1 char(32),\n"
                        + "   partparam2 char(32))";
                stmt = conn.createStatement();
                stmt.executeUpdate(query);
                PreparedStatement pstmt;
                pstmt = conn.prepareStatement("INSERT INTO ICS421.DTABLES (NODEDRIVER, NODEURL, NODEUSER, NODEPASSWD, NODEID) VALUES (?, ?, ?, ?, ?)");
                pstmt.setString(1, driver);
                pstmt.setString(2, url);
                pstmt.setString(3, userName);
                pstmt.setString(4, password);
                pstmt.setInt(5, 0);
                pstmt.executeUpdate();
                status = "updated";
            }
            
            stmt.close();
            conn.close();
            //System.out.println("Disconnected from database");
        } catch (Exception e) {
            e.printStackTrace();
            status = "update failed";
        } finally {
            System.out.println("[" + inputUrl + "]: catalog " + status + ".");
        }
    }

    private static boolean tableAlreadyExists(SQLException e) {
        boolean exists;
        if (e.getSQLState().equals("X0Y32")) {
            exists = true;
        } else {
            exists = false;
        }
        return exists;
    }

    private static class ThreadConnection implements Runnable {

        String inputDriver;
        String inputUrl;
        //String inputDbName;
        String inputUserName;
        String inputPassword;
        String ddl;

        private void ConnectToDerby(String inputDriver, String inputUrl, String inputUserName, String inputPassword) {
            //System.out.println("Connecting to Derby Database: " + inputUrl);
            Connection conn = null;
            String url = inputUrl;
            //String dbName = inputDbName;
            String driver = inputDriver;
            String userName = inputUserName;
            String password = inputPassword;
            String lines, query;
            StringBuilder line = new StringBuilder();
            Statement stmt;
            String status = null;
            try {
                Class.forName(driver).newInstance();
                conn = DriverManager.getConnection(inputUrl + ";create=true", userName, password);
                //System.out.println("Connected to the database " + inputUrl);
                
                /*
                query = "DROP TABLE BOOKS";
                try {
                    stmt = conn.createStatement();
                    stmt.executeUpdate(query);
                } catch (SQLException e) {
                    if (tableAlreadyExists(e)) {
                        return;
                    }
                }
                */
                
                BufferedReader br = new BufferedReader(new FileReader(ddl));
                
                while ((lines = br.readLine()) != null) {
                    line.append(lines);
                }
                if(line.lastIndexOf(";") > -1){
                    line.deleteCharAt(line.lastIndexOf(";"));
                }
                //System.out.println(line);
                query = line.toString();
                stmt = conn.createStatement();
                stmt.executeUpdate(query);
                


                status = "success";
                conn.close();
                //System.out.println("Disconnected from database" + inputUrl);
            } catch (Exception e) {
                status = "failed";
                return;
            } finally {
                System.out.println("[" + inputUrl + "]: " + ddl + " " +  status + ".");
            }
        }

        private ThreadConnection(String driver, String url, String userName, String passwrd, String ddl) {
            this.inputDriver = driver;
            this.inputUrl = url;
            //this.inputDbName = dbName;
            this.inputUserName = userName;
            this.inputPassword = passwrd;
            this.ddl = ddl;
        }

        @Override
        public void run() {
            this.ConnectToDerby(inputDriver, inputUrl, inputUserName, inputPassword);
        }
    }

    

}
