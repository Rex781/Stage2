/*
 * COMP3100: Assignment - Stage 1
 *
 * Authors: Francisco Butturini
 *
 */

import java.io.*;
import java.net.*;
import java.io.File;
import java.util.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;


public class DSClient {
    public static void main(String[] args) {
        Socket s = null;

        try {
            // Open a port to the server and input/output streams
            int serverPort = 50000;
            s = new Socket("localhost", serverPort);
            BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
            DataOutputStream out = new DataOutputStream(s.getOutputStream());

            // Setup a connection following the defined protocol
            handshake(in, out);

            if (((String)in.readLine()).equals("OK")) {
                Document doc = readFile("./ds-system.xml"); // Read ds-system.xml

                // Get a list of servers and thier coreCounts
                NodeList nList = doc.getElementsByTagName("server");   // Create a list of the server names
                String[][] servers = getServerList(nList);

                // Get the largest server
                int max_core_count = Integer.MIN_VALUE;
                String server_max = "";
                String server_id = "";
                for(int i = 0; i< servers.length; i++) {
                    if (Integer.parseInt(servers[i][1]) > max_core_count) {
                        max_core_count = Integer.parseInt(servers[i][1]);
                        server_max = servers[i][0];
                        server_id = "0";
                    }
                }

                // Preform scheduling
                String msg = " ";
                while (!msg.equals("NONE")) {

                    out.write(("REDY\n").getBytes());   // Ready for next job
                    //System.out.println("Sent REDY");

                    // Get the job
                    String job = in.readLine();
                    //System.out.println("Read this"+job);
                    String[] job_info = job.split(" ",0);

                    while (job_info[0].equals("JCPL")) {    // Disregard JCPL commands
                        out.write(("REDY\n".getBytes()));
                        job = in.readLine();
                        job_info = job.split(" ");
                    }

                    if (job.equals("NONE")) {
                        break;
                    }
                    //System.out.println("About to enter handle with "+job);
                    //handleGETS(job_info,in,out);
                    avail(job_info,in,out);
                    // Prepare and send schedule command
                    //String job_schedule = "SCHD" + " " + job_info[2] + " " + server_max + " " + server_id + "\n";
                    //out.write(job_schedule.getBytes());

                    msg = in.readLine();
                    //System.out.println("msg is"+msg);
                }
            }

            disconnect(s, in, out);
        }

        catch (Exception e) {
            System.out.println(e);
        }
    }

    public static void handleGETS(String [] job,BufferedReader din, DataOutputStream dout) {
        //System.out.println("got into handle" +job[1]+job[2]+job[3]+job[4]+job[5]+job[6]);
        String in;
        String [] inarr;
        try{
            String capable="GETS Capable "+job[4]+" "+job[5]+" "+job[6]+"\n";
            //System.out.println("Sent "+capable);
            dout.write(capable.getBytes());
            in=din.readLine();
            //System.out.println("Read "+in);
            inarr=in.split(" ");
            dout.write("OK\n".getBytes());
            String [] capableArray = new String [Integer.parseInt(inarr[1])];
            for(int i=0;i<Integer.parseInt(inarr[1]);i++){
               capableArray[i]=din.readLine();
               //System.out.println("Read in loop "+capableArray[i]);
            }

            int bestIDX=0;
            int bestCore=0;
            for(int j=0;j<Integer.parseInt(inarr[1]);j++){
                String [] test=capableArray[j].split(" ");
                if(Integer.parseInt(test[4])<bestCore){
                    bestIDX=j;
                    bestCore=Integer.parseInt(test[4]);
                }
             }

            dout.write("OK\n".getBytes());
            //System.out.println("Sent OK!");
            String check=din.readLine();// Negates the .
            //System.out.println(check+"!!!!!!!!!!");
            String []capableServer=capableArray[bestIDX].split(" ");
            String job_schedule = "SCHD" + " " + job[2] + " " + capableServer[0] + " " + capableServer[1] + "\n";
            //System.out.println("Sending job"+job_schedule);
            dout.write(job_schedule.getBytes());
            //System.out.println("Getting outta handle");

        }

        catch (Exception e) {
            System.out.println(e);
        }
    }

    public static void avail(String [] job,BufferedReader din, DataOutputStream dout) {
        String in;
        String [] inarr;
        try{
            String avail="GETS Avail "+job[4]+" "+job[5]+" "+job[6]+"\n";
            dout.write(avail.getBytes());
            in=din.readLine();
            //System.out.println(in);
            inarr=in.split(" ");
            //System.out.println(inarr[1]);
            if(inarr[1].equals("0")){
                //System.out.println("Entered loop!");
                dout.write("OK\n".getBytes());
                String check=din.readLine();//Negates the .
                //System.out.println(check);
                //System.out.println("!!!!passing to handle!!!!");
                handleGETS(job, din, dout);
                return;
            }
            //System.out.println("Didnt enter loop");
            inarr=in.split(" ");

            dout.write("OK\n".getBytes());

            String [] capableArray = new String [Integer.parseInt(inarr[1])];
            for(int i=0;i<Integer.parseInt(inarr[1]);i++){
               capableArray[i]=din.readLine();
               //System.out.println("Read in loop "+capableArray[i]);
            }
            int bestIDX=0;
            int bestCore=0;
            for(int j=0;j<Integer.parseInt(inarr[1]);j++){
                String [] test=capableArray[j].split(" ");
                if(Integer.parseInt(test[4])<bestCore){
                    bestIDX=j;
                    bestCore=Integer.parseInt(test[4]);
                }
             }

            dout.write("OK\n".getBytes());

            String check=din.readLine();// Negates the .

            String []capableServer=capableArray[bestIDX].split(" ");
            String job_schedule = "SCHD" + " " + job[2] + " " + capableServer[0] + " " + capableServer[1] + "\n";
            //System.out.println("Sending job"+job_schedule);
            dout.write(job_schedule.getBytes());
            //System.out.println("Getting outta avail");

        }
        catch (Exception e) {
            System.out.println(e);
        }
    }



    /* Performs the initial handshake
     *
     * din: An input stream to read from
     * dout: An output stream to write to
     */
    public static void handshake(BufferedReader din, DataOutputStream dout) {
        String user = System.getProperty("user.name");
        try {
            dout.write(("HELO\n").getBytes());

            String str = din.readLine();
            if (str.equals("OK"))
                dout.write(("AUTH " + user + "\n").getBytes());
        }

        catch (Exception e) {
            System.out.println(e);
        }
    }

    /* Reads the specified file and creates a document
     *
     * path: A file path for the file to be read
     *
     * returns: A document object of the file
     */
    public static Document readFile(String path) {
        try {
            File file = new File(path);
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document document = db.parse(file);
            document.getDocumentElement().normalize();
            return document;
        }

        catch (Exception e) {
            System.out.println(e);
            return null;
        }
    }

    /* Gets a list of servers names and thier coreCount
     *
     * nList: A NodeList to get data from
     *
     * returns: An array of server name, coreCount pairs.
     */
    public static String[][] getServerList(NodeList nList){
        String[][] xml = new String[nList.getLength()][2];
            for(int i =0 ; i< nList.getLength(); i++){
                Node nNode = nList.item(i);
                if(nNode.getNodeType()== Node.ELEMENT_NODE){
                    Element eElement = (Element) nNode;
                    xml[i][0]=eElement.getAttribute("type");
                    xml[i][1]= eElement.getAttribute("coreCount").toString();
                }
            }


        return xml;
    }

    /* Disconnect from the socket
     *
     * socket: A socket to disconnect from
     * din: An input stream to read from
     * dout: An output stream to write to
     */
    public static void disconnect(Socket socket, BufferedReader din, DataOutputStream dout) {
        try {
            dout.write(("QUIT\n").getBytes());

            if (((String)din.readLine()).equals("QUIT"))
                socket.close();  // Close the connection
        }

        catch (Exception e) {
            System.out.println(e);
        }
    }
}
