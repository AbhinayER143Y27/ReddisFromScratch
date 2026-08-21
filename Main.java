import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

class Main
{
    public static void main(String[] args) {
        int port = 6379;
        Scanner scanner = new Scanner(System.in);
        try (ServerSocket serversocket = new ServerSocket(port)) {
            while (true) {
                Socket socket = serversocket.accept();
                System.out.println("The connection has been build");
                InputStream stream = socket.getInputStream();

                BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
                System.out.println("The users request: " + reader.readLine());
                InputStream input = socket.getInputStream();
            }
        } catch (IOException e) {
            System.out.println("Error : " + e.getMessage());
        }
    }
}
