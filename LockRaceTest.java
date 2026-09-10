import java.io.*;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

public class LockRaceTest {

    static final String HOST = "localhost";
    static final int PORT = 6379;
    static final String LOCK_KEY = "lock:race";
    static final int NUM_CLIENTS = 100;
    static final int NUM_ROUNDS = 100;

    public static void main(String[] args) throws Exception {
        int totalSuccesses = 0;
        int roundsWithZeroWinners = 0;
        int roundsWithMultipleWinners = 0;

        for (int round = 1; round <= NUM_ROUNDS; round++) {
            cleanupKey(); // make sure the lock is free before each round

            AtomicInteger winners = new AtomicInteger(0);
            Thread[] threads = new Thread[NUM_CLIENTS];
            String[] results = new String[NUM_CLIENTS];

            for (int i = 0; i < NUM_CLIENTS; i++) {
                final int id = i;
                final String token = "token_" + id;
                threads[i] = new Thread(() -> {
                    String reply = sendSetNx(LOCK_KEY, token, 30000);
                    results[id] = token + " -> " + reply;
                    if (reply.trim().equals(":1")) {
                        winners.incrementAndGet();
                    }
                });
            }

            // start them all as close together as possible
            for (Thread t : threads) t.start();
            for (Thread t : threads) t.join();

            int winCount = winners.get();
            totalSuccesses += winCount;
            if (winCount == 0) roundsWithZeroWinners++;
            if (winCount > 1) roundsWithMultipleWinners++;

            System.out.println("Round " + round + ": winners=" + winCount);
            if (winCount != 1) {
                // print full detail only when something looks wrong
                for (String r : results) System.out.println("    " + r);
            }
        }

        System.out.println();
        System.out.println("=== SUMMARY ===");
        System.out.println("Rounds run: " + NUM_ROUNDS);
        System.out.println("Rounds with exactly 1 winner (correct): " + (NUM_ROUNDS - roundsWithZeroWinners - roundsWithMultipleWinners));
        System.out.println("Rounds with 0 winners (BUG - nobody got the lock): " + roundsWithZeroWinners);
        System.out.println("Rounds with >1 winners (BUG - two clients both got the lock): " + roundsWithMultipleWinners);
    }

    // Sends SET key value NX PX ttlMs as a RESP array and returns the raw reply line(s).
    static String sendSetNx(String key, String value, long ttlMs) {
        String[] parts = {"SET", key, value, "NX", "PX", String.valueOf(ttlMs)};
        try (Socket socket = new Socket(HOST, PORT)) {
            OutputStream out = socket.getOutputStream();
            out.write(encodeResp(parts));
            out.flush();

            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String line = in.readLine();
            return line == null ? "(no reply)" : line;
        } catch (IOException e) {
            return "(error: " + e.getMessage() + ")";
        }
    }

    // Encodes a command as a RESP array: *N\r\n$len\r\narg\r\n...
    static byte[] encodeResp(String[] parts) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(parts.length).append("\r\n");
        for (String p : parts) {
            sb.append("$").append(p.length()).append("\r\n");
            sb.append(p).append("\r\n");
        }
        return sb.toString().getBytes();
    }

    // Removes the lock key between rounds so each round starts clean.
    static void cleanupKey() {
        try (Socket socket = new Socket(HOST, PORT)) {
            OutputStream out = socket.getOutputStream();
            out.write(encodeResp(new String[]{"DEL", LOCK_KEY}));
            out.flush();
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            in.readLine(); // discard reply
        } catch (IOException ignored) {}
    }
}
