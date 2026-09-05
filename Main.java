import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

class Main
{
    private static ConcurrentHashMap<String, RedisObject> MainSets = new ConcurrentHashMap<>(); // for the data without the expiration.
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
                    System.out.println("In the for loop");
                    Long time = dataSets.get(x);
                    if(time == null)
                    {
                        counter++;
                        continue;
                    }
                    if(time < System.currentTimeMillis())
                    {
                        System.out.println(dataSets.get(x) + " is removed because of time limitations......");
                        dataSets.remove(x);
                        MainSets.remove(x);
                        aggressiveCounter++;
                    }
                    if(aggressiveCounter > 5)
                    {
                        continue outer;
                    }
                    counter++;
                    if(counter >= Math.min(20, keys.size()))
                    {
                        break;
                    }
                }
                try {
                    System.out.println("Sleeping now...");
                    Thread.sleep(3000); //look at this the thread in here is the deletion thread which has to go to sleep
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
                        Deque<String> listForLR = new LinkedList<>();
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

                                switch (command) {
                                    case "ECHO":
                                        if (collectedArgs.size() != 2) {
                                            output.write(("-There must be 2 inputs for the command ECHO.\r\n").getBytes());
                                            output.flush();
                                        } else {
                                            output.write(("$" + collectedArgs.get(1).length() + "\r\n").getBytes());
                                            output.write((collectedArgs.get(1) + "\r\n").getBytes());
                                            System.out.println("The users request " + collectedArgs.get(1));
                                            output.flush();
                                        }
                                        break;

                                    case "SET":
                                        int pxIndex = -1;
                                        for (int i = 0; i < collectedArgs.size(); i++) {
                                            if (collectedArgs.get(i).equalsIgnoreCase("PX")) {
                                                pxIndex = i;
                                                break;
                                            }
                                        }
                                        Long Time;
                                        if (pxIndex != -1 && collectedArgs.size() > pxIndex + 1) {
                                            // in this if && collectedArgs.size() > 3 this was added which was there now it is removed because what if set color px is written like this just a really great edge case in here for redis.
                                            Time = Long.parseLong(collectedArgs.get(pxIndex + 1));
                                            dataSets.put(collectedArgs.get(1), Time + System.currentTimeMillis());
                                            MainSets.put(collectedArgs.get(1), new RedisObject(RedisObject.Type.STRING, collectedArgs.get(2)));
                                            output.write(("+Ok\r\n").getBytes());
                                            output.flush();
                                        } else if (pxIndex == -1 && collectedArgs.size() >= 3) {
                                            String key = collectedArgs.get(1);
                                            dataSets.remove(key);
                                            MainSets.put(key, new RedisObject(RedisObject.Type.STRING, collectedArgs.get(2)));
                                            output.write(("+OK\r\n".getBytes()));
                                            output.flush();
                                        } else {
                                            output.write(("-There is problem in the manner of your writing.\r\n").getBytes());
                                            output.flush();
                                        }
                                        break;

                                    case "GET":
                                        if (collectedArgs.size() != 2) {
                                            output.write(("-There has to be 2 inputs for the command GET.\r\n".getBytes()));
                                            output.flush();
                                        } else {
                                            String key = collectedArgs.get(1);
                                            if (MainSets.containsKey(key)) {

                                                Long time = dataSets.get(key);
                                                if (MainSets.containsKey(key) && !dataSets.containsKey(key)) {
                                                    RedisObject getValue = MainSets.get(key);
                                                    Object valueobject = getValue.payLoad;
                                                    String value = "";
                                                    if (getValue.type == RedisObject.Type.STRING) {
                                                        value = String.valueOf(valueobject);
                                                    } else if (getValue.type == RedisObject.Type.LIST) {
                                                        // we will do this later because i cannot see in the future right so we will work around that and then i will see what to do in here
                                                        // the probability will be higher at that time when the list will be implemented or something like that ig.
                                                        //value = List.of();
                                                    }
                                                    output.write(("$" + value.length() + "\r\n").getBytes());
                                                    output.write((value + "\r\n").getBytes());
                                                    output.flush();
                                                } else if (dataSets.containsKey(key) && time < System.currentTimeMillis()) {
                                                    System.out.println("Key removed by the get call");
                                                    dataSets.remove(key);
                                                    MainSets.remove(key);
                                                    output.write(("$-1\r\n").getBytes());
                                                    output.flush();
                                                } else if (dataSets.containsKey(key) && time > System.currentTimeMillis()) {
                                                    RedisObject getValue = MainSets.get(key);
                                                    Object valueObject = getValue.payLoad;
                                                    String value = "";
                                                    if (getValue.type == RedisObject.Type.STRING) {
                                                        value = String.valueOf(valueObject);
                                                    }
                                                    output.write(("$" + value.length() + "\r\n").getBytes());
                                                    output.write((value + "\r\n").getBytes());
                                                    output.flush();
                                                }
                                            } else {
                                                output.write(("$-1\r\n").getBytes());
                                                output.flush();
                                            }
                                        }
                                        break;

                                    case "LPUSH":
                                        String keyL = collectedArgs.get(1);
                                        RedisObject existingL = MainSets.get(keyL);
                                        Deque<String> listL;
                                        if (existingL == null) {
                                            listL = new LinkedList<>();
                                            MainSets.put(keyL, new RedisObject(RedisObject.Type.LIST, listL));
                                        } else if (existingL != null && existingL.type == RedisObject.Type.LIST) // so that means the current key is not a string but a list so
                                        {
                                            listL = (Deque<String>) existingL.payLoad;
                                        } else {
                                            output.write(("$-1\r\n").getBytes());
                                            output.flush();
                                            break;
                                        }

                                        for (int i = 2; i < collectedArgs.size(); i++) {
                                            listL.addFirst(collectedArgs.get(i));
                                        }

                                        output.write(("+OK\r\n".getBytes()));
                                        output.flush();
                                        break;

                                    case "RPUSH":
                                        String keyR = collectedArgs.get(1);
                                        RedisObject existingR = MainSets.get(keyR);
                                        Deque<String> listR;
                                        if (existingR == null) {
                                            listR = new LinkedList<>();
                                            MainSets.put(keyR, new RedisObject(RedisObject.Type.LIST, listR));
                                        } else if (existingR != null && existingR.type == RedisObject.Type.LIST) {
                                            listR = (Deque<String>) existingR.payLoad;
                                        } else {
                                            output.write(("$-1\r\n").getBytes());
                                            output.flush();
                                            break;
                                        }
                                        for (int i = 2; i < collectedArgs.size(); i++) {
                                            listR.addLast(collectedArgs.get(i));
                                        }
                                        output.write(("+OK\r\n").getBytes());
                                        output.flush();
                                        break;

                                    case "LPOP":
                                        if (collectedArgs.size() != 2) {
                                            String erpop = "Less arguments given.";
                                            output.write(("$-" + erpop.length() + "\r\n").getBytes());
                                            output.write((erpop + "\r\n").getBytes());
                                            output.flush();
                                            break;
                                        }
                                        else {
                                            String keyLP = collectedArgs.get(1);
                                            RedisObject existingLP = MainSets.get(keyLP);
                                            if (existingLP == null) {
                                                output.write(("$-1\r\n").getBytes());
                                                output.flush();
                                            } else if(existingLP.type == RedisObject.Type.LIST) {
                                                Deque<String> listLP = (Deque<String>) existingLP.payLoad;
                                                String value = listLP.pollFirst();
                                                if (value == null) {
                                                    output.write(("$-1\r\n").getBytes());
                                                    output.flush();
                                                } else {
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
                                            break;
                                        }

                                    case "RPOP":
                                        if (collectedArgs.size() != 2) {
                                            String erpop = "Less arguments given.";
                                            output.write(("$" + erpop.length() + "\r\n").getBytes());
                                            output.write((erpop + "\r\n").getBytes());
                                            output.flush();
                                            break;
                                        }
                                        else {
                                            String keyRP = collectedArgs.get(1);
                                            RedisObject existingRP = MainSets.get(keyRP);
                                            if (existingRP == null) {
                                                output.write(("$-1\r\n").getBytes());
                                                output.flush();
                                            } else if(existingRP.type == RedisObject.Type.LIST){
                                                Deque<String> listRP = (Deque<String>) existingRP.payLoad;
                                                String valueRP = listRP.pollLast();
                                                if (valueRP == null) {
                                                    output.write(("$-1\r\n").getBytes());
                                                    output.flush();
                                                } else {
                                                    output.write(("$" + valueRP.length() + "\r\n").getBytes());
                                                    output.write((valueRP + "\r\n").getBytes());
                                                    output.flush();
                                                }
                                            }
                                            else
                                            {
                                                output.write(("$-1\r\n").getBytes());
                                                output.flush();
                                            }
                                            break;
                                        }

                                    case "LRANGE":
                                        if(collectedArgs.size() != 4)
                                        {
                                            String erRan = "Wrong amount of arguments given";
                                            output.write(("$" + erRan + "\r\n").getBytes());
                                            output.write((erRan + "\r\n").getBytes());
                                        }
                                        else
                                        {
                                            String keyLange = collectedArgs.get(1);
                                            RedisObject existingLange = MainSets.get(keyLange);
                                            if(existingLange == null)
                                            {
                                                output.write(("*0\r\n").getBytes());
                                                output.flush();
                                            }
                                            else if(existingLange.type == RedisObject.Type.LIST)
                                            {
                                                int startLange = 0;
                                                int endLange = 0;
                                                try{startLange = Integer.parseInt(collectedArgs.get(2));
                                                    endLange = Integer.parseInt(collectedArgs.get(3));}
                                                catch (NumberFormatException e)
                                                {
                                                    String mathEror = "input wasn't a valid integer.";
                                                    output.write(("$" + mathEror.length() + "\r\n").getBytes());
                                                    output.write((mathEror + "\r\n").getBytes());
                                                    output.flush();
                                                    break;
                                                }
                                                List<String> listLange = (List<String>)existingLange.payLoad;

                                                if(startLange < 0)
                                                {
                                                    startLange = listLange.size() + startLange;
                                                }
                                                if(startLange < 0)
                                                {
                                                    startLange = 0;
                                                }
                                                if(endLange < 0)
                                                {
                                                    endLange = listLange.size() + endLange;
                                                }
                                                if(endLange < 0)
                                                {
                                                    endLange = 0;
                                                }
                                                if(startLange > endLange)
                                                {
                                                    output.write(("*0\r\n").getBytes());
                                                    output.flush();
                                                }
                                                else if(startLange <= endLange)
                                                {
                                                    int endPoint = Math.min(endLange, listLange.size() - 1);
                                                    int returnNum = endPoint - startLange + 1;
                                                    output.write(("*" + returnNum + "\r\n").getBytes());
                                                    for(int i = startLange; i <= endPoint; i++)
                                                    {
                                                        output.write(("$" + listLange.get(i).length() + "\r\n").getBytes());
                                                        output.write((listLange.get(i) + "\r\n").getBytes());
                                                    }
                                                    output.flush();
                                                }
                                            }
                                            else
                                            {
                                                output.write(("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").getBytes());
                                                output.flush();
                                            }
                                        }
                                        break;

                                    case "LLEN":
                                        String keyLen = collectedArgs.get(1);
                                        RedisObject existingLen = MainSets.get(keyLen);
                                        if(existingLen == null)
                                        {
                                            output.write((":0\r\n").getBytes()); // it is 0 not 1 for the non-existing key.
                                            output.flush();
                                        } else if(existingLen.type == RedisObject.Type.LIST)
                                        {
                                            List<String> listLen = (List<String>) existingLen.payLoad;
                                            int listLenLength = listLen.size();
                                            output.write((":" + listLenLength + "\r\n").getBytes());
                                            output.flush();
                                        }
                                        else
                                        {
                                            output.write(("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").getBytes()); // it is 0 not 1 for the non-existing key.
                                            output.flush();
                                        }
                                        break;

                                    case "DEL":
                                        int counterDel = 0;
                                        for(int i = 1; i < collectedArgs.size(); i++)
                                        {
                                            String keyDel = collectedArgs.get(i);
                                            RedisObject removeDel = MainSets.remove(keyDel);
                                            dataSets.remove(keyDel);
                                            if(removeDel != null)
                                            {
                                                counterDel++;
                                            }
                                        }
                                        output.write((":" + counterDel + "\r\n").getBytes());
                                        output.flush();
                                        break;

                                    case "EXISTS":
                                        int countExist = 0;
                                        for(int i = 1; i < collectedArgs.size(); i++)
                                        {
                                            String keyExist = collectedArgs.get(i);
                                            if(MainSets.containsKey(keyExist))
                                            {
                                                countExist++;
                                            }
                                        }
                                        output.write((":" + countExist + "\r\n").getBytes());
                                        output.flush();
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

class RedisObject
{
    enum Type
    {
        STRING,
        LIST,
        HASH,
        SET
    }

    final Type type;
    final Object payLoad;

    public RedisObject(Type type ,Object payLoad)
    {
        this.type = type;
        this.payLoad = payLoad;
    }
}
