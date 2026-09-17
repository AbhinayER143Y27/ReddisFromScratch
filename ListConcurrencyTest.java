import java.io.*;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ListConcurrencyTest {

    static final String HOST = "localhost";
    static final int PORT = 6379;
    static final int NUM_THREADS = 20;
    static final int PUSHES_PER_THREAD = 50;
    static final String KEY = "concurrency:list";

    public static void main(String[] args) throws Exception {
        // clean slate before the run
        try (Socket control = new Socket(HOST, PORT)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(control.getInputStream()));
            sendCommand(control.getOutputStream(), "DEL", KEY);
            reader.readLine(); // discard reply
        }

        ExecutorService pool = Executors.newFixedThreadPool(NUM_THREADS);
        CountDownLatch latch = new CountDownLatch(NUM_THREADS);

        for (int t = 0; t < NUM_THREADS; t++) {
            final int id = t;
            pool.submit(() -> {
                try (Socket socket = new Socket(HOST, PORT)) {
                    OutputStream out = socket.getOutputStream();
                    // one BufferedReader, created once, reused for every push on this connection
                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    for (int i = 0; i < PUSHES_PER_THREAD; i++) {
                        String value = "t" + id + "_v" + i;
                        // alternate LPUSH/RPUSH so both code paths get exercised under the same lock
                        String cmd = (i % 2 == 0) ? "LPUSH" : "RPUSH";
                        sendCommand(out, cmd, KEY, value);
                        reader.readLine(); // must drain this reply before the next send, or replies desync
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        pool.shutdown();

        int expected = NUM_THREADS * PUSHES_PER_THREAD;

        try (Socket control = new Socket(HOST, PORT)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(control.getInputStream()));
            sendCommand(control.getOutputStream(), "LLEN", KEY);
            String reply = reader.readLine(); // e.g. ":1000"
            int actual = Integer.parseInt(reply.substring(1));

            System.out.println("Expected length: " + expected);
            System.out.println("Actual length:   " + actual);
            System.out.println(actual == expected
                    ? "PASS - no pushes were lost"
                    : "FAIL - lost writes under concurrent LPUSH/RPUSH (missing lock protection or a race)");
        }
    }

    static void sendCommand(OutputStream out, String... parts) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(parts.length).append("\r\n");
        for (String p : parts) {
            sb.append("$").append(p.length()).append("\r\n").append(p).append("\r\n");
        }
        out.write(sb.toString().getBytes());
        out.flush();
    }
}
