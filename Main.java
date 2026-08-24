import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Time;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

class Main
{
    private static ConcurrentHashMap<String, StoredValue> dataSets = new ConcurrentHashMap<>();
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
                        while (true) {
                            String line = reader.readLine();
                            if (line == null) break;

                            if (line.equals("exit")) {
                                break;
                            }

                            if(line.startsWith("*"))
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
                                if(collectedArgs.isEmpty()) continue;

                                String command = collectedArgs.get(0).toUpperCase();

                                switch (command)
                                {
                                    case "ECHO":
                                        if(collectedArgs.size() != 2)
                                        {
                                            output.write(("-There must be 2 inputs for the command ECHO.\r\n").getBytes());
                                            output.flush();
                                        }
                                        else
                                        {
                                            output.write(("$" + collectedArgs.get(1).length() + "\r\n").getBytes());
                                            output.write((collectedArgs.get(1) + "\r\n").getBytes());
                                            System.out.println("The users request " + collectedArgs.get(1));
                                            output.flush();
                                        }
                                        break;

                                    case "SET":
                                        if(collectedArgs.size() != 3)
                                        {
                                            output.write(("-There has to be 3 inputs for the command SET").getBytes());
                                            output.flush();
                                        }
                                        else
                                        {
                                            long Time = 0;
                                            dataSets.put(collectedArgs.get(1), new StoredValue(collectedArgs.get(2), Time));
                                            output.write(("+OK\r\n".getBytes()));
                                        }
                                        break;

                                    case "GET":
                                        if(collectedArgs.size() != 2)
                                        {
                                            output.write(("-There has to be 2 inputs for the command GET.\r\n".getBytes()));
                                        }
                                        else
                                        {
                                            String print = dataSets.get(collectedArgs.get(1));
                                            if(print == null)
                                            {
                                                output.write(("$-1\r\n").getBytes());
                                            }
                                            else {
                                                output.write(("+" + print + "\r\n").getBytes());
                                                output.flush();
                                            }
                                        }
                                        break;

                                    case "PING":
                                        output.write(("+PONG\r\n").getBytes());
                                        output.flush();
                                        break;

                                    default:
                                        String error = "- Error unknown command " + command + "\r\n";
                                        output.write((error).getBytes());
                                        output.flush();
                                        break;
                                }
                                collectedArgs.clear();
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
class StoredValue
{
   final String Data;
   final long Time;

   public StoredValue(String Data, long Time)
   {
       this.Data = Data;
       this.Time = (Time > 0) ? (System.currentTimeMillis() + Time) : -1;
   }
}
