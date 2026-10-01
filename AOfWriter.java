import java.io.*;
import java.util.ArrayList;
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
}
