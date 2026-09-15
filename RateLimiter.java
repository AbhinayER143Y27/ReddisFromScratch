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
    int TokenBucketLimit = 10;
    int TokenPerSec = 2;

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

    public boolean tryAcquire(String key) // skip this i am creating this currently
    {
        sendCommand("Get", key);
        String ans = getCommand();
        long currentToken;
        long lastRefillMS;
        if(ans == null) // if readline is used then even if it is true bez of readline it will be false bez that creates a new object.
        {
            currentToken = 10;
            lastRefillMS = System.currentTimeMillis();
        }
        else
        {
            String[] splitAns = ans.split(":"); // token : timeStamps
            currentToken =  Long.parseLong(splitAns[0]);
            lastRefillMS = Long.parseLong(splitAns[1]);
            double something = System.currentTimeMillis() - lastRefillMS;
            int am  = (int)((something / 1000) * TokenPerSec);
            currentToken = Math.min(TokenBucketLimit ,(currentToken + am));
        }
    }

    String getCommand()
    {
        try
        {
            String dataReturned = dataFromServer.readLine();
            if(dataReturned.startsWith("$-1"))
            {
                return null;
            }
            else
            {
                if(dataReturned.startsWith("$"))
                {
                    String value = dataFromServer.readLine();
                    return value;
                }
            }
            return dataReturned;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
