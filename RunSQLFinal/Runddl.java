
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class Runddl {

    private static int nthreads = 0;
    private static String driver = null;
    private static String connUrl = null;
    private static String connUser = null;
    private static String connPwd = null;
    private static List<ClusterInfo> list = new ArrayList<ClusterInfo>();

    public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException {
        
        for(int i = 0; i < args.length; i++){
            System.out.println(args[i]);
        }
        
        /* ************* MAIN PROGRAM **************/
        Runddl dt = new Runddl();
        ClusterInfo node = new ClusterInfo();

        dt.readCfg(args[0]);
        Thread[] tList = new Thread[nthreads];

        String ddlFile = args[1];

        for (int i = 0; i < nthreads; i++) {
            node = list.get(i);
            tList[i] = new Thread(new ThreadConnection(node.getNodedriver(), node.getNodeurl(), node.getNodeuser(), node.getNodepasswd(), ddlFile));
            tList[i].start();
        }
        for (int ij = 0; ij < nthreads; ij++) {
            tList[ij].join();
        }

    }

    private void readCfg(String filePath) throws FileNotFoundException, IOException {
        int currentid = 2;
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        try {
            String line = null, driver = null, hostname = null, username = null, passwd = null;
            String cdriver = null, chostname = null, cusername = null, cpasswd = null;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("catalog")) {
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    cdriver = line;
                    this.driver = cdriver;
                    //System.out.println(cdriver);
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
            Class.forName(this.driver).newInstance();
            conn = DriverManager.getConnection(this.connUrl + "?verifyServerCertificate=false&useSSL=true", this.connUser, this.connPwd);
            String query = "INSERT INTO DTABLES (NODEDRIVER, NODEURL, NODEUSER, NODEPASSWD, NODEID) VALUES ('" + 
                    driver+ "', '" + url + "', '"+ userName + "', '" + password + "', " + id + ")";
            //System.out.println(query);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(query);
            conn.close();
        } catch (Exception e) {
            //System.out.println("[" + this.connUrl + "]: Cluster Node Update Failed.");
            return;
        }
    }

    private void ReadCatalog(String inputDriver, String inputUrl, String inputUserName, String inputPassword) {
        Connection conn = null;
        String url = inputUrl;
        String driver = inputDriver;
        String userName = inputUserName;
        String password = inputPassword;
        String status = null;
        try {
            Class.forName(this.driver).newInstance();
            conn = DriverManager.getConnection(this.connUrl + "?verifyServerCertificate=false&useSSL=true", this.connUser, this.connPwd);
     
            String query;
            Statement stmt = null;

            query = "CREATE TABLE IF NOT EXISTS DTABLES (tname char(32), \n"
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
            query = "INSERT INTO DTABLES (NODEDRIVER, NODEURL, NODEUSER, NODEPASSWD, NODEID) VALUES ('" + 
                    this.driver+ "', '" + this.connUrl + "', '"+ this.connUser + "', '" + this.connPwd + "', " + 1 + ")";
            //System.out.println(query);
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            status = "updated";
            //}

            stmt.close();
            conn.close();
        } catch (Exception e) {
            //e.printStackTrace();
            status = "update failed";
            return;
        } finally {
            System.out.println("[" + inputUrl + "]: catalog " + status + ".");
        }
    }

    private static class ThreadConnection implements Runnable {

        String inputDriver;
        String inputUrl;
        String inputUserName;
        String inputPassword;
        String ddl;

        private void ConnectToDerby(String inputDriver, String inputUrl, String inputUserName, String inputPassword) {
            Connection conn = null;
            String lines, query;
            StringBuilder line = new StringBuilder();
            Statement stmt;
            String status = null;
            try {
                Class.forName(inputDriver).newInstance();
                conn = DriverManager.getConnection(inputUrl + "?verifyServerCertificate=false&useSSL=true", inputUserName, inputPassword);
                
                BufferedReader br = new BufferedReader(new FileReader(ddl));

                while ((lines = br.readLine()) != null) {
                    line.append(lines);
                }
                if (line.lastIndexOf(";") > -1) {
                    line.deleteCharAt(line.lastIndexOf(";"));
                }
                query = line.toString();
                stmt = conn.createStatement();
                stmt.executeUpdate(query);
                
                StringTokenizer st = new StringTokenizer(query, " ");
                String tableName = null;
                while(st.hasMoreTokens()){
                    tableName = st.nextToken();
                    if(st.nextToken().contains("(")){
                        break;
                    }
                }
                //System.out.println(tableName);
                
                Class.forName(driver).newInstance();
                Connection connn = DriverManager.getConnection(connUrl + 
                        "?verifyServerCertificate=false&useSSL=true", connUser, connPwd);
                
                String Query = "UPDATE DTABLES SET TNAME = '"+ tableName + "' WHERE NODEURL = '" + inputUrl + "'";
                Statement stmt1 = connn.createStatement();
                stmt1.executeUpdate(Query);
                connn.close();
                
                status = "success";
                conn.close();
            } catch (Exception e) {
                status = "failed";
                return;
            } finally {
                System.out.println("[" + inputUrl + "]: " + ddl + " " + status + ".");
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
