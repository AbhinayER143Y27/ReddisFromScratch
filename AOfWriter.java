import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

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
}
