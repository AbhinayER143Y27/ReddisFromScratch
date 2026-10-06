import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AOfWriter {
    private final BufferedWriter writer;
    private final Object FileWriterLock = new Object();


    public AOfWriter(File filePath) throws IOException
    {
        this.writer = new BufferedWriter(new FileWriter(filePath, true)); // Use no string here only filePath
    }

    public void logCommand(String ... parts) throws IOException {
        synchronized (FileWriterLock) {
            writer.write(String.join(" ", parts));
            writer.newLine();
            writer.flush();
        }
    }

    public List<String[]> replayFromOf(File log) throws IOException
    {
        BufferedReader fileReader = new BufferedReader(new FileReader(log));
        List<String[]> logFileData = new ArrayList<String[]>();
        while(true) {
            String txt = fileReader.readLine();
            if(txt == null)break;
            String[] s = txt.split(" ");
            logFileData.add(s);
        }
        fileReader.close();
        return logFileData;
    }

    public void readingTowards(List<String[]> theList)
    {
        for(String[] parts : theList) {
            String commandInHere = parts[0].toUpperCase();
                    switch (commandInHere) {
                        case "SET":
                            if(parts.length == 3)
                            {
                                Main.MainSets.put(parts[1],new RedisObject(RedisObject.Type.STRING, parts[2]));
                            }
                            if(parts.length == 5)
                            {
                                long time = Long.parseLong(parts[4]);
                                if(time > System.currentTimeMillis())
                                {
                                    Main.MainSets.put(parts[1],new RedisObject(RedisObject.Type.STRING, parts[2]));
                                    Main.dataSets.put(parts[1], time);
                                }
                            }
                        case "DEL":
                            for(int i = 1; i < parts.length; i++) {
                                Main.MainSets.remove(parts[i]);
                                Main.dataSets.remove(parts[i]);
                            }
                    }
        }
    }
}
