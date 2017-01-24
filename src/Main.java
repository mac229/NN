import java.io.*;
import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;

public class Main {

    private static final Object sync = new Object();
    private static int[] a;
    private static int[] b;
    private static final int PRIME_NUMBER = 1_100_009;// First Prime Number Greater Than Biggest Identifier

    private static final int NUMBER_HASH_FUNCTION = 4;
    private static final int NUMBER_ROW_PER_BAND = 2;

    private static void initializeHashFunctions(int universeSize) {
        a = new int[NUMBER_HASH_FUNCTION];
        b = new int[NUMBER_HASH_FUNCTION];

        Random rand = new Random();
        for (int i = 0; i < NUMBER_HASH_FUNCTION; i++) {
            a[i] = rand.nextInt(universeSize) + 1;
            b[i] = rand.nextInt(universeSize);
        }
    }

    private static int getHash(int a, int b, int p, int input) {
        long i = (long) a * input;
        long j = i + b;
        return (int) (j % p);
    }

    private static int[] getMinHash(Set<Integer> songs) {
        int[] minHashes = new int[NUMBER_HASH_FUNCTION];
        for (int i = 0; i < NUMBER_HASH_FUNCTION; i++) {
            minHashes[i] = Integer.MAX_VALUE;
        }

        for (int songId : songs) {
            for (int i = 0; i < NUMBER_HASH_FUNCTION; i++) {
                int hash = getHash(a[i], b[i], PRIME_NUMBER, songId);
                if (hash < 0) {
                    System.out.println("alarm");
                }
                minHashes[i] = Math.min(hash, minHashes[i]);
            }
        }
        return minHashes;
    }

    public static void main(String[] args) throws IOException {
        initializeHashFunctions(PRIME_NUMBER);

        long start = System.currentTimeMillis();
        Map<Integer, HashSet<Integer>> users = loadUsers();
        System.out.println("Users size " + users.size());
        System.out.println("Reading time " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();
        HashMap<Integer, int[]> signatureMatrix = getSignatureMatrix(users);
        System.out.println("Signature time " + (System.currentTimeMillis() - start));

        users.clear();
        users = null;

        start = System.currentTimeMillis();
        HashMap<Integer, Set<Integer>> buckets = getBuckets(signatureMatrix);
        System.out.println("Buckets time " + (System.currentTimeMillis() - start));
        System.out.println("Buckets size " + buckets.size());

        start = System.currentTimeMillis();
        Map<Integer, Set<Integer>> filtered = filter(buckets);
        System.out.println("Filter time " + (System.currentTimeMillis() - start));
        System.out.println("Filter size " + filtered.size());

        buckets.clear();
        buckets = null;

        start = System.currentTimeMillis();
        getNN(filtered);
        System.out.println("Fill NN time " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();
        //HashMap<Integer, HashMap<Integer, Double>> similarity = computeSimilarity(NN, signatureMatrix);
        System.out.println("Similarity time " + (System.currentTimeMillis() - start));

        //saveResults(similarity);
        System.out.println("DONE");
    }

    private static HashMap<Integer, HashMap<Integer, Double>> computeSimilarity(HashMap<Integer, HashSet<Integer>> NN, HashMap<Integer, List<Integer>> signatures) {
        HashMap<Integer, HashMap<Integer, Double>> similarity = new HashMap<>();

        NN.entrySet().parallelStream().filter(n -> !n.getValue().isEmpty()).forEach(nn ->
        {
            synchronized (sync) {
                similarity.put(nn.getKey(), new HashMap<>());
            }

            for (int userId : nn.getValue()) {
                int intersectCount = 0;
                int unionCount = signatures.get(userId).size() + signatures.get(nn.getKey()).size();

                if (signatures.get(userId).size() < signatures.get(nn.getKey()).size()) {
                    for (Integer currentSong : signatures.get(userId)) {
                        if (signatures.get(nn.getKey()).contains(currentSong))
                            intersectCount++;
                    }
                } else {
                    for (Integer currentSong : signatures.get(nn.getKey())) {
                        if (signatures.get(userId).contains(currentSong))
                            intersectCount++;
                    }
                }

                if (intersectCount > 0) {
                    synchronized (sync) {
                        similarity.get(nn.getKey()).put(userId, ((double) intersectCount / (unionCount - intersectCount)));
                    }
                }
            }
        });
        return similarity;
    }

    private static HashMap<Integer, HashMap<Integer, Double>> computeSimilarity(Map<Integer, HashSet<Integer>> users, HashMap<Integer, HashSet<Integer>> NN) {
        HashMap<Integer, HashMap<Integer, Double>> similarity = new HashMap<>();

        NN.entrySet().parallelStream().filter(n -> !n.getValue().isEmpty()).forEach(nn ->
        {
            synchronized (sync) {
                similarity.put(nn.getKey(), new HashMap<>());
            }

            for (Integer userId : nn.getValue()) {
                int samehashes = 0;
                int sumOfSizes = users.get(userId).size() + users.get(nn.getKey()).size();

                if (users.get(userId).size() < users.get(nn.getKey()).size()) {
                    for (Integer currentSong : users.get(userId)) {
                        if (users.get(nn.getKey()).contains(currentSong))
                            samehashes++;
                    }
                } else {
                    for (Integer currentSong : users.get(nn.getKey())) {
                        if (users.get(userId).contains(currentSong))
                            samehashes++;
                    }
                }
                //////----------8<-----------------
                if (samehashes > 0) {
                    synchronized (sync) {
                        similarity.get(nn.getKey()).put(userId, ((double) samehashes / (sumOfSizes - samehashes)));
                    }
                }
            }
        });
        return similarity;
    }

    private static HashMap<Integer, HashSet<Integer>> initializeNN(Set<Integer> usersIds) {
        HashMap<Integer, HashSet<Integer>> NN = new HashMap<>(usersIds.size());
        for (int userId : usersIds) {
            NN.put(userId, new HashSet<>());
        }
        return NN;
    }

    private static Map<Integer, Set<Integer>> filter(Map<Integer, Set<Integer>> buckets) {
        return buckets.entrySet().stream().filter(entry -> entry.getValue().size() > 1).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    // <userId, set<nearest users>
    private static Map<Integer, Set<Integer>> getNN(Map<Integer, Set<Integer>> buckets) {
        Map<Integer, Set<Integer>> nn = new HashMap<>();
        long multi = 0;
        HashSet<Integer> users = new HashSet<>();
        for (Map.Entry<Integer, Set<Integer>> hash : buckets.entrySet()) {
            for (Integer userId : hash.getValue()) {
                if (nn.get(userId) == null) {
                    nn.put(userId, new HashSet<>());
                }
                //nn.get(userId).addAll(hash.getValue());
                users.addAll(hash.getValue());
            }

            multi += hash.getValue().size();
        }

        System.out.println("multi " + multi);
        System.out.println("users " + users.size());
        System.out.println("nn " + nn.size());
        return nn;
    }

    // <Hash, List of users>
    private static HashMap<Integer, Set<Integer>> getBuckets(HashMap<Integer, int[]> signatureMatrix) {
        int size = NUMBER_HASH_FUNCTION / NUMBER_ROW_PER_BAND;
        int counter = 0;
        HashMap<Integer, Set<Integer>> buckets = new HashMap<>();
        for (int i = 0; i < size; i++) {
            System.out.println(i);
            int band = NUMBER_ROW_PER_BAND * i;
            StringBuilder builder = new StringBuilder();
            for (Integer userId : signatureMatrix.keySet()) {
                builder.setLength(0);
                for (int j = 0; j < NUMBER_ROW_PER_BAND; j++) {
                    builder.append(signatureMatrix.get(userId)[band + j]).append(",");
                }

                int hash = builder.toString().hashCode();

                if (!buckets.containsKey(hash)) {
                    buckets.put(hash, new HashSet<>());
                }

                buckets.get(hash).add(userId);
                counter++;
            }
        }

        return buckets;
    }

    // <userId, List of hashes>
    private static HashMap<Integer, int[]> getSignatureMatrix(Map<Integer, HashSet<Integer>> users) {
        HashMap<Integer, int[]> signatureMatrix = new HashMap<>();
        users.entrySet().parallelStream().forEach(user ->
        {
            int[] hash = getMinHash(user.getValue());
            synchronized (sync) {
                signatureMatrix.put(user.getKey(), hash);
            }
        });
        return signatureMatrix;
    }

    ////////////////////

    private static void saveResults(HashMap<Integer, HashMap<Integer, Double>> similarity) throws IOException {
        Writer output = new BufferedWriter(new FileWriter("result.txt"));
        for (Map.Entry<Integer, HashMap<Integer, Double>> sim : similarity.entrySet().stream().limit(100).collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue())).entrySet()) {
            if (sim.getValue().size() > 0) {
                String resultString = "User\t" + sim.getKey() + ":\n";

                for (Map.Entry<Integer, Double> u : sim.getValue().entrySet().stream().sorted(Map.Entry.<Integer, Double>comparingByValue().reversed()).limit(100).collect(Collectors.toList())) {
                    if (u.getValue() == 1) {
                        //    continue;
                    }

                    resultString += "\t" + u.getKey() + "\t" + new DecimalFormat("#.###").format(u.getValue()) + "\n";
                    output.append(resultString);
                }
            }

        }
        output.close();
    }

    private static HashMap<Integer, HashMap<Integer, Double>> getSimilarity(Map<Integer, HashSet<Integer>> users) throws IOException {
        HashMap<Integer, HashMap<Integer, Double>> similarity = new HashMap<>();
        users.entrySet().parallelStream().limit(100).forEach(currentUser -> {
            synchronized (sync) {
                similarity.put(currentUser.getKey(), new HashMap<>());
            }

            // <userId, <songsIds>
            for (Map.Entry<Integer, HashSet<Integer>> user : users.entrySet()) {
                if (user.getKey().equals(currentUser.getKey()))
                    continue;
                else {
                    int sameSongs = 0;

                    if (currentUser.getValue().size() < user.getValue().size()) {
                        for (Integer currentSong : currentUser.getValue()) {
                            if (user.getValue().contains(currentSong))
                                sameSongs++;
                        }
                    } else {
                        for (Integer currentSong : user.getValue()) {
                            if (currentUser.getValue().contains(currentSong))
                                sameSongs++;
                        }
                    }

                    if (sameSongs > 0) {
                        synchronized (sync) {
                            double sim = ((double) sameSongs / ((currentUser.getValue().size() + user.getValue().size()) - sameSongs));
                            similarity.get(currentUser.getKey()).put(user.getKey(), sim);
                        }
                    }
                }
            }
        });

        return similarity;
    }

    private static HashMap<Integer, HashSet<Integer>> loadUsers() throws IOException {
        HashMap<Integer, HashSet<Integer>> users = new HashMap<>();
        InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream("./facts.csv"), "ISO-8859-1");
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String line = bufferedReader.readLine();
        while ((line = bufferedReader.readLine()) != null) {
            String[] values = line.split(",");
            int songId = Integer.parseInt(values[0]);
            int userId = Integer.parseInt(values[1]);

            if (users.containsKey(userId)) {
                if (!users.get(userId).contains(songId))
                    users.get(userId).add(songId);
            } else {
                HashSet<Integer> hashSet = new HashSet<Integer>();
                hashSet.add(songId);
                users.put(userId, hashSet);
            }
        }
        bufferedReader.close();
        return users;
    }

}
