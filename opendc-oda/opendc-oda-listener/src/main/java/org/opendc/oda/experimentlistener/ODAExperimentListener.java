package org.opendc.oda.experimentlistener;

import org.opendc.oda.experimentrunner.SchedulingAlgorithmComparatorExperiment;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * This class creates the server-side socket, waiting for connection from ODAbler
 * , suggesting it to execute various possible simulations
 */
public class ODAExperimentListener {
    //static ServerSocket variable
    private static ServerSocket server;
    //socket server port on which it will listen
    private static final int PORT = 10000;
    static boolean EXIT_MESSAGE = false;
    public static void main(String[] args) {
        //startApplicationServer();
        SchedulingAlgorithmComparatorExperiment exp = new SchedulingAlgorithmComparatorExperiment();
        exp.triggerExperiment();
    }

    private static void startApplicationServer() throws IOException {
        //create the socket server object
        server = new ServerSocket(PORT);

        Socket socket = null;
        try {
            //keep listens indefinitely until receives 'exit' call or program terminates
            while (!EXIT_MESSAGE) {
                System.out.println("Waiting for the client request");
                //creating socket and waiting for client connection
                socket = server.accept();
                Thread thread = new ODAblerConnectionHandler(socket, socket.getInputStream(), socket.getOutputStream());
                // starting thread
                thread.start();
                // if(message.equalsIgnoreCase("exit")) break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Shutting down Socket server!!");
            //close the ServerSocket object
            server.close();
        }
    }
}

class ODAblerConnectionHandler extends Thread{

    final Socket socket;
    final InputStream in;
    final OutputStream out;
    public ODAblerConnectionHandler(Socket s, InputStream in, OutputStream out) {
        this.socket = s;
        this.in = in;
        this.out = out;
    }

    @Override
    public void run()
    {
        String message;
        System.out.println("Testing");
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        //create ObjectOutputStream object
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
        try{
            message = reader.readLine();
            System.out.println("Message Received: " + message);
            //write object to Socket
            writer.write("Hi Client - I am done!");
            writer.newLine();
            writer.flush();
            //close resources if termination signal received
            // and terminate the server once client sends termination request
            if(message.equalsIgnoreCase("TERMINATE")) {
                writer.write("Shutting down OpenDC experiment server!!");
                writer.newLine();
                writer.flush();
                reader.close();
                writer.close();
                in.close();
                out.close();
                socket.close();
                ODAExperimentListener.EXIT_MESSAGE = true;
                System.out.println("Shut down done");
            }
            else{
                if(message.equalsIgnoreCase("ENERGY")){

                }
                else if(message.equalsIgnoreCase("ANOMALY")){

                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
