import java.io.*;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

public class LockRaceTest {

    static final String HOST = "localhost";
    static final int PORT = 6379;
    static final String LOCK_KEY = "lock:race";
    static final int NUM_CLIENTS = 100;
    static final int NUM_ROUNDS = 100;

    // one persistent connection per client, opened once and reused for every round
    static Socket[] clientSockets = new Socket[NUM_CLIENTS];
    static BufferedReader[] clientReaders = new BufferedReader[NUM_CLIENTS];
    static OutputStream[] clientOutputs = new OutputStream[NUM_CLIENTS];

    // one persistent connection for cleanup between rounds
    static Socket cleanupSocket;
    static BufferedReader cleanupReader;
    static OutputStream cleanupOutput;

    public static void main(String[] args) throws Exception {
        int totalSuccesses = 0;
        int roundsWithZeroWinners = 0;
        int roundsWithMultipleWinners = 0;

        // open every connection ONCE, up front
        for (int i = 0; i < NUM_CLIENTS; i++) {
            clientSockets[i] = new Socket(HOST, PORT);
            clientOutputs[i] = clientSockets[i].getOutputStream();
            clientReaders[i] = new BufferedReader(new InputStreamReader(clientSockets[i].getInputStream()));
        }
        cleanupSocket = new Socket(HOST, PORT);
        cleanupOutput = cleanupSocket.getOutputStream();
        cleanupReader = new BufferedReader(new InputStreamReader(cleanupSocket.getInputStream()));

        try {
            for (int round = 1; round <= NUM_ROUNDS; round++) {
                cleanupKey(); // make sure the lock is free before each round

                AtomicInteger winners = new AtomicInteger(0);
                Thread[] threads = new Thread[NUM_CLIENTS];
                String[] results = new String[NUM_CLIENTS];

                for (int i = 0; i < NUM_CLIENTS; i++) {
                    final int id = i;
                    final String token = "token_" + id;
                    threads[i] = new Thread(() -> {
                        String reply = sendSetNx(id, LOCK_KEY, token, 30000);
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
        } finally {
            // close every connection ONCE, at the very end
            for (Socket s : clientSockets) {
                try { s.close(); } catch (IOException ignored) {}
            }
            try { cleanupSocket.close(); } catch (IOException ignored) {}
        }

        System.out.println();
        System.out.println("=== SUMMARY ===");
        System.out.println("Rounds run: " + NUM_ROUNDS);
        System.out.println("Rounds with exactly 1 winner (correct): " + (NUM_ROUNDS - roundsWithZeroWinners - roundsWithMultipleWinners));
        System.out.println("Rounds with 0 winners (BUG - nobody got the lock): " + roundsWithZeroWinners);
        System.out.println("Rounds with >1 winners (BUG - two clients both got the lock): " + roundsWithMultipleWinners);
    }

    // Sends SET key value NX PX ttlMs over client id's already-open connection.
    static String sendSetNx(int clientId, String key, String value, long ttlMs) {
        String[] parts = {"SET", key, value, "NX", "PX", String.valueOf(ttlMs)};
        try {
            clientOutputs[clientId].write(encodeResp(parts));
            clientOutputs[clientId].flush();
            String line = clientReaders[clientId].readLine();
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

    // Removes the lock key between rounds, over the one persistent cleanup connection.
    static void cleanupKey() {
        try {
            cleanupOutput.write(encodeResp(new String[]{"DEL", LOCK_KEY}));
            cleanupOutput.flush();
            cleanupReader.readLine(); // discard reply
        } catch (IOException ignored) {}
    }
}
