import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RateLimiter {
    static final String HOST = "localhost";
    static final int PORT = 6379;
    Socket socket;
    InputStream inputStream;
    OutputStream outputStream;


    public RateLimiter(String words)
    {
        try {
            socket = new Socket(HOST, PORT);
            inputStream = socket.getInputStream();
            outputStream = socket.getOutputStream();

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
}
