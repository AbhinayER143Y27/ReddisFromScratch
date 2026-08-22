import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

class Main
{
    public static void main(String[] args) {
        int port = 6379;
        try (ServerSocket serversocket = new ServerSocket(port)) {
            while (true) {
                Socket socket = serversocket.accept();
                Thread thread = new Thread(() -> {
                    try {
                        System.out.println("The connection has been build");
                        InputStream stream = socket.getInputStream();
                        OutputStream output = socket.getOutputStream();
                        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));

                        while (true) {
                            String line = reader.readLine();
                            if (line == null) break;
                            if (line.equals("COMMAND")) {
                                output.write("+OK\r\n".getBytes());
                                output.flush();
                            }
                            if (line.equals("PING")) {
                                output.write("+PONG\r\n".getBytes());
                                output.flush();
                            }
                            if (line.equals("exit")) {
                                break;
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
