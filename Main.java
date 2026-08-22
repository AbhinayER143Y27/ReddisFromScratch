import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

class Main
{
    public static void main(String[] args) {
        int port = 6379;
        try (ServerSocket serversocket = new ServerSocket(port)) {
            while (true) {
                Socket socket = serversocket.accept();
                Thread thread = new Thread(() -> {//for the 10,000 threads the CPU will spend more time swapping between threads than actually doing the work - context switching overhead.
                    try {
                        System.out.println("The connection has been build");
                        InputStream stream = socket.getInputStream();
                        OutputStream output = socket.getOutputStream();
                        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
                        ArrayList<String> collectedArgs = new ArrayList<>();
                        boolean commandTrue = false;
                        while (true) {
                            String line = reader.readLine();
                            if (line == null) break;
                            if (line.equals("COMMAND")) {
                                output.write("+OK\r\n".getBytes());
                                output.flush();
                                commandTrue = true;
                            }
                            if (line.equals("exit")) {
                                break;
                            }

                            if(line.startsWith("*") && commandTrue)
                            {
                                int arrayNumber = Integer.parseInt(line.substring(1));
                                for(int i = 0; i < 2 * arrayNumber; i++)
                                {
                                    String insideLine = reader.readLine();
                                    if(insideLine.startsWith("$"))
                                    {
                                        continue;
                                    }
                                    else{
                                        collectedArgs.add(insideLine);
                                    }
                                }

                                if(collectedArgs.get(0).equals("ECHO"))
                                {
                                    for(int i = 1; i < collectedArgs.size(); i++)
                                    {
                                        output.write(("$" + collectedArgs.get(i).length() + "\r\n").getBytes());
                                        output.write((collectedArgs.get(i) + "\r\n").getBytes());
                                    }
                                }
                            }
                            if (line.equals("PING")) {
                                output.write("+PONG\r\n".getBytes());
                                output.flush();
                            }
                            if(collectedArgs.contains("PING"))
                            {
                                System.out.println("The users request: PING");
                                output.write("+PONG\r\n".getBytes());
                                output.flush();
                            }
                            System.out.println("The users request: " + line);
                        }
                    }
                    catch(IOException e)
                    {
                        System.out.println(e.getMessage());
                    }
                });
                thread.start();
            }
        } catch(IOException e){
                System.out.println("Error : " + e.getMessage());
        }
    }
}
