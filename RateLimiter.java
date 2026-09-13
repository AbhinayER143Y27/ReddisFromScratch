import java.io.*;
import java.net.Socket;
import java.util.Arrays;

public class RateLimiter {
    static final String HOST = "localhost";
    static final int PORT = 6379;
    Socket socket;
    InputStream inputStream;
    OutputStream outputStream;
    BufferedReader dataFromServer;

    public RateLimiter()
    {
        try {
            socket = new Socket(HOST, PORT);
            inputStream = socket.getInputStream();
            outputStream = socket.getOutputStream();
            dataFromServer = new BufferedReader(new InputStreamReader(inputStream)); // this will be the part of the get command which will be called many times
            //making each call wrap every input stream into a brand new buffered reader which then actually does its own internal buffering making things go worse.
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void sendCommand(String... parts)
    {
        try
        {
            outputStream.write(("*" + parts.length + "\r\n").getBytes());

            for(String part : parts)
            {
                outputStream.write(("$" + part.length() + "\r\n").getBytes());
                outputStream.write((part + "\r\n").getBytes());
            }
            outputStream.flush();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    void tryAcquire() // skip this i am creating this currently
    {
        String key = null;
        try {
            key = dataFromServer.readLine();
            sendCommand("Get", key);
            String ans = getCommand();
            if(ans == "$-1")
            {
                // we will put the bucket to max value;
            }
            else
            {
                ans.split(":");
            }


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    String getCommand()
    {
        try
        {
            String dataReturned = dataFromServer.readLine();
            System.out.println(dataReturned);
            return dataReturned;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
