import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

class Main
{
    private static ConcurrentHashMap<String, String> MainSets = new ConcurrentHashMap<>(); // for the data without the expiration.
    private static ConcurrentHashMap<String, Long> dataSets = new ConcurrentHashMap<>(); // for the data with the expiration.

    public static void main(String[] args) {
        int port = 6379;

            Thread deletionThread = new Thread(() ->
            {
                outer: while(true) {
                    ArrayList<String> keys = new ArrayList<>(dataSets.keySet()); //each cycle will get a new snapshot of the keys in the list.
                    Collections.shuffle(keys);
                    int counter = 0;
                    int aggressiveCounter = 0;
                    for(String x : keys)
                    {
                        Long time = dataSets.get(x);
                        if(time == null)
                        {
                            counter++;
                            continue;
                        }
                        if(time < System.currentTimeMillis())
                        {
                            dataSets.remove(x);
                            MainSets.remove(x);
                            aggressiveCounter++;
                        }
                        counter++;
                        if(counter >= Math.min(20, keys.size()))
                        {
                            break;
                        }
                        if(aggressiveCounter > 5)
                        {
                            //counter = 0;
                        }
                    }
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });


        try (ServerSocket serversocket = new ServerSocket(port)) {

            deletionThread.start();

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
                                        if(collectedArgs.size() == 5 && collectedArgs.get(3).equalsIgnoreCase("PX"))
                                        {
                                            Long timer = Long.parseLong(collectedArgs.get(4));
                                            dataSets.put(collectedArgs.get(1), timer);
                                            MainSets.put(collectedArgs.get(1), collectedArgs.get(2));
                                            output.write(("+Ok\r\n").getBytes());
                                            output.flush();
                                        }
                                        else if(collectedArgs.size() == 3)
                                        {
                                            String key = collectedArgs.get(1);
                                            dataSets.remove(key);
                                            MainSets.put(key, collectedArgs.get(2));
                                            output.write(("+OK\r\n".getBytes()));
                                            output.flush();
                                        }
                                        else
                                        {
                                            output.write(("-There has to be 3 inputs for the command SET\r\n").getBytes());
                                            output.flush();
                                        }
                                        break;

                                    case "GET":
                                        if(collectedArgs.size() != 2)
                                        {
                                            output.write(("-There has to be 2 inputs for the command GET.\r\n".getBytes()));
                                            output.flush();
                                        }
                                        else
                                        {
                                            String key = collectedArgs.get(1);
                                            if(MainSets.containsKey(key))
                                            {
                                                Long time = dataSets.get(key);
                                                if(MainSets.containsKey(key) && !dataSets.containsKey(key))
                                                {
                                                    String value = MainSets.get(key);
                                                    output.write(("$" + value.length() + "\r\n").getBytes());
                                                    output.write((value + "\r\n").getBytes());
                                                    output.flush();
                                                }
                                                else if(dataSets.containsKey(key) && time < System.currentTimeMillis())
                                                {
                                                    dataSets.remove(key);
                                                    MainSets.remove(key);
                                                    output.write(("$-1\r\n").getBytes());
                                                    output.flush();
                                                }
                                                else if(dataSets.containsKey(key) && time > System.currentTimeMillis())
                                                {
                                                    String value = MainSets.get(key);
                                                    output.write(("$" + value.length() + "\r\n").getBytes());
                                                    output.write((value + "\r\n").getBytes());
                                                    output.flush();
                                                }
                                            }
                                            else
                                            {
                                                output.write(("$-1\r\n").getBytes());
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
