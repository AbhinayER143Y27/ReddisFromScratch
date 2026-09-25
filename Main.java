import RateLimiterr.RateLimiter;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

class Main
{
    private static ConcurrentHashMap<String, RedisObject> MainSets = new ConcurrentHashMap<>(); // for the data without the expiration.
    private static ConcurrentHashMap<String, Long> dataSets = new ConcurrentHashMap<>(); // for the data with the expiration.
    private static final Object lockGuard = new Object();
    static File logFile = new File("log.txt");
    private static AOfWriter fileWriting;

    public static void main(String[] args) {
        int port = 6379;
         try{fileWriting = new AOfWriter(logFile);}
         catch (IOException e){
             System.out.println("Failed to open file: " + e.getMessage());}
            Thread deletionThread = new Thread(() ->
            {
                outer: while(true) {
                    ArrayList<String> keys = new ArrayList<>(dataSets.keySet()); //each cycle will get a new snapshot of the keys in the list.
                    Collections.shuffle(keys);
                    int counter = 0;
                    int aggressiveCounter = 0;
                    for(String x : keys)
                    {
                        synchronized (lockGuard) {
                            Long time = dataSets.get(x);
                            if (time == null) {
                                counter++;
                                continue;
                            }
                            if (time < System.currentTimeMillis()) {
                                dataSets.remove(x);
                                MainSets.remove(x);
                                aggressiveCounter++;
                            }
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
                        //System.out.println("Sleeping now...");
                        Thread.sleep(200); //look at this the thread in here is the deletion thread which has to go to sleep
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });


        try (ServerSocket serversocket = new ServerSocket(port)) {

            deletionThread.start();
            RateLimiter rateLimiter = new RateLimiter();
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

                                    case "SET": // ex - seconds ,px - milli seconds
                                        int pxIndex = -1;
                                        int exIndex = -1;
                                        int nxIndex = -1;
                                        int xxIndex = -1;
                                        for (int i = 3; i < collectedArgs.size(); i++) {
                                            if(collectedArgs.get(i).equalsIgnoreCase("NX"))
                                            {
                                                nxIndex = i;
                                            }
                                            if(collectedArgs.get(i).equalsIgnoreCase("XX"))
                                            {
                                                xxIndex = i;
                                            }
                                            if (collectedArgs.get(i).equalsIgnoreCase("PX")) {
                                                pxIndex = i;
                                            }
                                            if(collectedArgs.get(i).equalsIgnoreCase("EX"))
                                            {
                                                exIndex = i;
                                            }
                                        }
                                        Long Time;

                                        // <------------------ NX Condition ---------------------> 1 means that the key doesn't exist and the key is placed and 0 means the key is already there.

                                        if (nxIndex != -1)
                                        {
                                            if (pxIndex != -1 && collectedArgs.size() > pxIndex + 1 && exIndex == -1) { // case for valid px
                                                Time = Long.parseLong(collectedArgs.get(pxIndex + 1)) + System.currentTimeMillis();
                                                String key = collectedArgs.get(1);
                                                collectedArgs.set(1,key);
                                                boolean acquired = false;
                                                synchronized (lockGuard) {
                                                    if (!MainSets.containsKey(key)) {
                                                        MainSets.put(key, new RedisObject(RedisObject.Type.STRING, collectedArgs.get(2)));
                                                        dataSets.put(key, Time);
                                                        acquired = true;
                                                    } else {
                                                        acquired = false;
                                                    }
                                                }
                                                fileWriting.logCommand(collectedArgs.toArray(new String[0]));
                                                    output.write((acquired ? ":1\r\n" : ":0\r\n").getBytes());
                                                    output.flush();
                                            } else if (exIndex != -1 && collectedArgs.size() > exIndex + 1 && pxIndex == -1)// case for valid ex
                                            {
                                                Time = Long.parseLong(collectedArgs.get(exIndex + 1));
                                                Time = (Time * 1000) + System.currentTimeMillis();
                                                collectedArgs.set(exIndex + 1,String.valueOf(Time));
                                                String key = collectedArgs.get(1);
                                                boolean acquired = false;
                                                synchronized (lockGuard) {
                                                    if(!MainSets.containsKey(key)) {
                                                        MainSets.put(key, new RedisObject(RedisObject.Type.STRING, collectedArgs.get(2)));
                                                        dataSets.put(key, Time);
                                                        acquired = true;
                                                    }
                                                    else {
                                                        acquired = false;
                                                        }
                                                }
                                                fileWriting.logCommand(collectedArgs.toArray(new String[0]));
                                                output.write((acquired ? ":1\r\n" : ":0\r\n").getBytes());
                                                output.flush();
                                            }
                                            else if (pxIndex == -1 && collectedArgs.size() == 4 && exIndex == -1) { // no extra so only 4 this cannot be used with the next one == 3 as that would be a problem
                                                String key = collectedArgs.get(1);
                                                RedisObject previous = MainSets.putIfAbsent(key, new RedisObject(RedisObject.Type.STRING, collectedArgs.get(2)));
                                                if(previous == null) {
                                                    fileWriting.logCommand(collectedArgs.toArray(new String[0]));
                                                    output.write((":1\r\n".getBytes()));
                                                }
                                                else
                                                {
                                                    output.write((":0\r\n").getBytes());
                                                }
                                                output.flush();
                                            }
                                        }

                                        // <--------------------------- XX Condition  -------------------------------->

                                        else if (xxIndex != -1) { // in here the 0 the key exists doesn't exist so 0 as a failure else 1.
                                            if (pxIndex != -1 && collectedArgs.size() > pxIndex + 1 && exIndex == -1) { // case for valid px
                                                Time = Long.parseLong(collectedArgs.get(pxIndex + 1));
                                                Time = Time + System.currentTimeMillis();
                                                collectedArgs.set(pxIndex + 1, String.valueOf(Time));
                                                String key = collectedArgs.get(1);
                                                boolean acquired = false;
                                                synchronized (lockGuard) {
                                                    RedisObject previousM = MainSets.replace(key, new RedisObject(RedisObject.Type.STRING, collectedArgs.get(2)));
                                                    if (previousM != null) {
                                                        dataSets.put(collectedArgs.get(1), Time);
                                                        acquired = true;
                                                    } else {
                                                        acquired = false;
                                                    }
                                                }
                                                fileWriting.logCommand(collectedArgs.toArray(new String[0]));
                                                output.write((acquired ? ":1\r\n" : ":0\r\n").getBytes());
                                                output.flush();
                                            } else if (exIndex != -1 && collectedArgs.size() > exIndex + 1 && pxIndex == -1)// case for valid ex
                                            {
                                                Time = Long.parseLong(collectedArgs.get(exIndex + 1));
                                                Time = (Time * 1000) + System.currentTimeMillis();
                                                collectedArgs.set(exIndex + 1, String.valueOf(Time));
                                                String key = collectedArgs.get(1);
                                                boolean acquired = false;
                                                synchronized (lockGuard) {
                                                    RedisObject previousM = MainSets.replace(key, new RedisObject(RedisObject.Type.STRING, collectedArgs.get(2)));
                                                    if (previousM != null) {
                                                        dataSets.put(key, Time);
                                                        acquired = true;
                                                    } else {
                                                    acquired = false;
                                                    }
                                                }
                                                if(acquired)fileWriting.logCommand(collectedArgs.toArray(new String[0]));
                                                output.write((acquired ? ":1\r\n" : ":0\r\n").getBytes());
                                                output.flush();
                                            }else if (pxIndex == -1 && collectedArgs.size() == 4 && exIndex == -1) { // no extra so only 4 this cannot be used with the next one == 3 as that would be a problem
                                                String key = collectedArgs.get(1);
                                                boolean acquired = false;
                                                synchronized (lockGuard) {
                                                    RedisObject previousM = MainSets.replace(key, new RedisObject(RedisObject.Type.STRING, collectedArgs.get(2)));
                                                    if (previousM != null) {
                                                        acquired = true;
                                                    } else {
                                                        acquired = false;
                                                    }
                                                }
                                                if(acquired) fileWriting.logCommand(collectedArgs.toArray(new String[0]));
                                                output.write((acquired ? ":1\r\n" : ":0\r\n").getBytes());
                                                output.flush();
                                            }
                                        }

                                        // <-----------------------   Normal One     -------------------------->
                                        else if (pxIndex != -1 && collectedArgs.size() > pxIndex + 1 && exIndex == -1) { // case for valid px
                                            // in this if && collectedArgs.size() > 3 this was added which was there now it is removed because what if set color px is written like this just a really great edge case in here for redis.
                                            Time = Long.parseLong(collectedArgs.get(pxIndex + 1));
                                            Time = Time + System.currentTimeMillis();
                                            collectedArgs.set(pxIndex + 1, String.valueOf(Time));
                                            synchronized (lockGuard) {
                                                dataSets.put(collectedArgs.get(1),Time);
                                                MainSets.put(collectedArgs.get(1), new RedisObject(RedisObject.Type.STRING, collectedArgs.get(2)));
                                            }
                                            fileWriting.logCommand(collectedArgs.toArray(new String[0]));
                                            output.write(("+Ok\r\n").getBytes());
                                            output.flush();
                                        }
                                        else if (exIndex != -1 && collectedArgs.size() > exIndex + 1 && pxIndex == -1)// case for valid ex
                                        {
                                            Time = Long.parseLong(collectedArgs.get(exIndex + 1));
                                            Time = (Time * 1000) + System.currentTimeMillis();
                                            collectedArgs.set(exIndex + 1, String.valueOf(Time));
                                            synchronized (lockGuard) {
                                                dataSets.put(collectedArgs.get(1),Time );
                                                MainSets.put(collectedArgs.get(1), new RedisObject(RedisObject.Type.STRING, collectedArgs.get(2)));
                                            }
                                            fileWriting.logCommand(collectedArgs.toArray(new String[0]));
                                            output.write(("+Ok\r\n").getBytes());
                                            output.flush();
                                        }
                                        else if (pxIndex == -1 && collectedArgs.size() == 3 && exIndex == -1 ) { // valid case for the set name abhinay
                                            String key = collectedArgs.get(1);
                                            synchronized (lockGuard) {
                                                dataSets.remove(key);
                                                MainSets.put(key, new RedisObject(RedisObject.Type.STRING, collectedArgs.get(2)));
                                            }
                                            fileWriting.logCommand(collectedArgs.toArray(new String[0]));
                                            output.write(("+OK\r\n".getBytes()));
                                            output.flush();
                                        } else if (pxIndex != -1 && exIndex != -1) {
                                            output.write(("-ERR syntax error\r\n").getBytes());
                                            output.flush();
                                        }

                                        else {
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
                                            boolean found = false;
                                            String value = null;
                                                synchronized (lockGuard)
                                                {
                                                    RedisObject obj = MainSets.get(key);
                                                    if(obj != null)
                                                    {
                                                        Long time = dataSets.get(key);
                                                        if(time != null && time < System.currentTimeMillis())
                                                        {
                                                            dataSets.remove(key);
                                                            MainSets.remove(key);
                                                        }
                                                        else
                                                        {
                                                            if(obj.type == RedisObject.Type.STRING) {
                                                                found = true;
                                                                value = String.valueOf(obj.payLoad);
                                                            }
                                                            else
                                                            {
                                                                output.write(("-WRONGTYPE Operation against a key holding the wrong kind of value\r" +
                                                                        "\n").getBytes());
                                                                output.flush();
                                                                break;
                                                            }
                                                        }
                                                    }
                                                }
                                                if(found)
                                                {
                                                    output.write(("$" + value.length() + "\r\n").getBytes());
                                                    output.write((value + "\r\n").getBytes());
                                                }
                                                else
                                                {
                                                    output.write(("$-1\r\n").getBytes());
                                                }
                                            output.flush();
                                        }
                                        break;

                                    case "LPUSH":
                                        String keyL = collectedArgs.get(1);
                                        RedisObject existingL;
                                        Deque<String> listL = null;
                                        boolean wrongType = false;
                                        synchronized (lockGuard) {
                                            existingL = MainSets.get(keyL);
                                            if (existingL == null) {
                                                listL = new LinkedList<>();
                                                MainSets.put(keyL, new RedisObject(RedisObject.Type.LIST, listL));
                                            } else if (existingL.type == RedisObject.Type.LIST) // so that means the current key is not a string but a list so
                                            {
                                                listL = (Deque<String>) existingL.payLoad;
                                            }
                                            else {
                                                wrongType = true;
                                            }
                                            if(!wrongType)
                                            {
                                                for (int i = 2; i < collectedArgs.size(); i++) {
                                                    listL.addFirst(collectedArgs.get(i));
                                                }
                                            }
                                        }
                                        if(wrongType)
                                        {
                                            output.write(("$-1\r\n").getBytes());
                                        }
                                        else
                                        {
                                            output.write(("+OK\r\n").getBytes());
                                        }
                                        output.flush();
                                        break;

                                    case "RPUSH":
                                        String keyR = collectedArgs.get(1);
                                        RedisObject existingR;
                                        Deque<String> listR = null;
                                        boolean wrType = false;
                                        synchronized (lockGuard) {
                                            existingR = MainSets.get(keyR);
                                            if (existingR == null) {
                                                listR = new LinkedList<>();
                                                MainSets.put(keyR, new RedisObject(RedisObject.Type.LIST, listR));
                                            } else if (existingR != null && existingR.type == RedisObject.Type.LIST) {
                                                listR = (Deque<String>) existingR.payLoad;
                                            } else {
                                                wrType = true;
                                            }
                                            if(!wrType){
                                            for (int i = 2; i < collectedArgs.size(); i++) {
                                                listR.addLast(collectedArgs.get(i));
                                            }}
                                        }
                                        if(wrType == true)
                                        {
                                            output.write(("$-1\r\n").getBytes());
                                        }
                                        else{
                                        output.write(("+OK\r\n").getBytes());}
                                        output.flush();
                                        break;

                                    case "LPOP":
                                        if (collectedArgs.size() != 2) {
                                            String erpop = "Less arguments given.";
                                            output.write(("-" + erpop + "\r\n").getBytes());
                                            output.flush();
                                            break;
                                        }
                                        else {
                                            String keyLP = collectedArgs.get(1);
                                            String value = "";
                                            boolean wrongThing = false;
                                            boolean rightThing = false;
                                            synchronized (lockGuard) {
                                                RedisObject existingLP = MainSets.get(keyLP);
                                                if (existingLP == null) {
                                                    wrongThing = true;
                                                } else if (existingLP.type == RedisObject.Type.LIST) {
                                                    Deque<String> listLP = (Deque<String>) existingLP.payLoad;
                                                    value = listLP.pollFirst();
                                                    if (value == null) {
                                                        wrongThing = true;
                                                    } else {
                                                        rightThing = true;
                                                    }
                                                } else {
                                                    wrongThing = true;
                                                }
                                            }
                                            if(wrongThing)
                                            {
                                                output.write(("$-1\r\n").getBytes());
                                            }
                                            if(rightThing)
                                            {
                                                output.write(("$" + value.length() + "\r\n").getBytes());
                                                output.write((value + "\r\n").getBytes());
                                            }
                                            output.flush();
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
                                            String valueRP = "";
                                            boolean wrongThing = false;
                                            boolean rightThing = false;
                                            synchronized (lockGuard) {
                                                RedisObject existingRP = MainSets.get(keyRP);
                                                if (existingRP == null) {
                                                    wrongThing = true;
                                                } else if (existingRP.type == RedisObject.Type.LIST) {
                                                    Deque<String> listRP = (Deque<String>) existingRP.payLoad;
                                                    valueRP = listRP.pollLast();
                                                    if (valueRP == null) {
                                                        wrongThing = true;
                                                    } else {
                                                        rightThing = true;
                                                    }
                                                } else {
                                                    wrongThing = true;
                                                }
                                            }
                                            if(wrongThing)
                                            {
                                                output.write(("$-1\r\n").getBytes());
                                            }
                                            if(rightThing) {
                                                output.write(("$" + valueRP.length() + "\r\n").getBytes());
                                                output.write((valueRP + "\r\n").getBytes());
                                            }
                                            output.flush();
                                        }
                                        break;

                                    case "LRANGE":
                                        if(collectedArgs.size() != 4)
                                        {
                                            String erRan = "Wrong amount of arguments given";
                                            output.write(("-" + erRan + "\r\n").getBytes());
                                            output.flush();
                                        }
                                        else
                                        {
                                            String keyLange = collectedArgs.get(1);
                                            boolean catchit = false;
                                            boolean wrongthing = false;
                                            boolean forloop = false;
                                            boolean mainerror = false;
                                            int startLange = 0;
                                            int endLange = 0;
                                            List<String> listLange = null;
                                            List<String> snapShot = null;
                                            synchronized (lockGuard) {
                                                RedisObject existingLange = MainSets.get(keyLange);
                                                if (existingLange == null) {
                                                    wrongthing = true;
                                                } else if (existingLange.type == RedisObject.Type.LIST) {
                                                    try {
                                                        startLange = Integer.parseInt(collectedArgs.get(2));
                                                        endLange = Integer.parseInt(collectedArgs.get(3));
                                                    } catch (NumberFormatException e) {
                                                        catchit = true;
                                                    }
                                                    if(!catchit) {
                                                        listLange = (List<String>) existingLange.payLoad;

                                                        if (startLange < 0) {
                                                            startLange = listLange.size() + startLange;
                                                        }
                                                        if (startLange < 0) {
                                                            startLange = 0;
                                                        }
                                                        if (endLange < 0) {
                                                            endLange = listLange.size() + endLange;
                                                        }
                                                        if (endLange < 0) {
                                                            endLange = 0;
                                                        }
                                                        if (startLange > endLange) {
                                                            wrongthing = true;
                                                        } else if (startLange <= endLange) {
                                                            int endPoint = Math.min(endLange, listLange.size() - 1);
                                                            forloop = true;
                                                            snapShot = new ArrayList<>(listLange.subList(startLange, endPoint + 1));
                                                        }
                                                    }
                                                }
                                                else
                                                {
                                                    mainerror = true;
                                                }
                                            }
                                            if(mainerror)
                                            {
                                                output.write(("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").getBytes());
                                            }

                                            if(catchit)
                                            {
                                                String mathEror = "input wasn't a valid integer.";
                                                output.write(("$" + mathEror.length() + "\r\n").getBytes());
                                                output.write((mathEror + "\r\n").getBytes());
                                            }
                                            if(forloop)
                                            {
                                                output.write(("*" + snapShot.size() + "\r\n").getBytes());
                                                for(int i = 0; i < snapShot.size(); i++)
                                                {
                                                    output.write(("$" + snapShot.get(i).length() + "\r\n").getBytes());
                                                    output.write((snapShot.get(i) + "\r\n").getBytes());
                                                }
                                            }
                                            if(wrongthing)
                                            {
                                                output.write(("*0\r\n").getBytes());
                                            }
                                            output.flush();
                                        }
                                        break;

                                    case "LLEN": // previously no syn needed because even after that the data will be stale
                                        // but now it is syn because for the atomicity.
                                        String keyLen = collectedArgs.get(1);
                                        int listLenLength = 0;
                                        boolean zero = false;
                                        boolean errorL = false;
                                        boolean ans = false;
                                        synchronized (lockGuard) {
                                            RedisObject existingLen = MainSets.get(keyLen);
                                            if (existingLen == null) {
                                                 zero = true;// it is 0 not 1 for the non-existing key.
                                            } else if (existingLen.type == RedisObject.Type.LIST) {
                                                List<String> listLen = (List<String>) existingLen.payLoad;
                                                listLenLength = listLen.size();
                                                ans = true;
                                            } else {
                                                errorL = true; // it is 0 not 1 for the non-existing key.
                                            }
                                        }
                                        if(zero) output.write((":0\r\n").getBytes());
                                        if(errorL) output.write(("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").getBytes());
                                        if(ans)output.write((":" + listLenLength + "\r\n").getBytes());
                                        output.flush();
                                        break;

                                    case "DEL":
                                        int counterDel = 0;
                                        for(int i = 1; i < collectedArgs.size(); i++)
                                        {
                                            String keyDel = collectedArgs.get(i);
                                            RedisObject removeDel;
                                            synchronized (lockGuard) {
                                                removeDel = MainSets.remove(keyDel);
                                                dataSets.remove(keyDel);
                                            }
                                            if (removeDel != null) {
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

                                    case "TTL": // -1 no expiry    -2 not found
                                        if(collectedArgs.size() != 2)
                                        {
                                            output.write(("-Wrong amount of arguments given.\r\n").getBytes());
                                            output.flush();
                                            break;
                                        }
                                        boolean One = false;
                                        boolean Two = false;
                                        boolean Different = false;
                                        Long existingTimeCurrent = null;
                                        synchronized (lockGuard) {
                                            String keyTTL = collectedArgs.get(1);
                                            if (MainSets.containsKey(keyTTL) && !dataSets.containsKey(keyTTL)) {
                                                One = true;
                                            } else if (MainSets.containsKey(keyTTL) && dataSets.containsKey(keyTTL)) {
                                                Long existingTime = dataSets.get(keyTTL);
                                                existingTimeCurrent = (existingTime - System.currentTimeMillis()) / 1000;
                                                if (existingTimeCurrent > 0) {
                                                    Different = true;
                                                } else {
                                                    MainSets.remove(keyTTL);
                                                    dataSets.remove(keyTTL);
                                                    Two = true;
                                                }
                                            } else {
                                                Two = true;
                                            }
                                        }
                                        if(One)output.write((":-1\r\n").getBytes());
                                        if(Two)output.write((":-2\r\n").getBytes());
                                        if(Different)output.write((":" + existingTimeCurrent + "\r\n").getBytes());
                                        output.flush();
                                        break;

                                    case "RELEASE":
                                        String key = collectedArgs.get(1);
                                        String Token = collectedArgs.get(2);
                                        boolean acquired = false;
                                        synchronized (lockGuard) {
                                            if (MainSets.remove(key, new RedisObject(RedisObject.Type.STRING, Token))) {
                                                dataSets.remove(key); // if false then the key next exists, means the lock was acquired without a PX, nothing to clean up, not a sign of expiry or any prior problem.
                                                acquired = true;
                                            } else {
                                                acquired = false;
                                            }
                                        }
                                        output.write((acquired ? "+OK\r\n" : "-Error value not owned.\r\n").getBytes());
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
                    finally {
                        try
                        {
                            socket.close();
                        }catch (IOException ignored) {}
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

    public boolean equals(Object other)
    {
        if(this == other) return true; // two references pointing to the same object, if it is same object not equal then simply return.

        if (other == null || getClass() != other.getClass()) { // deciding that two objects are not equal
            return false;
        }

        //no class cast exception in here
        RedisObject obj = (RedisObject) other; //this doesn't give the other access to the same object it gives the another reference of the  object but with more specific type.
        return type == obj.type && Objects.equals(payLoad,obj.payLoad);
    }

    @Override
    public int hashCode(){
        return Objects.hash(type, payLoad);
    }
}
