import java.io.*;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

public class Main {

    private static final Object sync = new Object();

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        Map<Integer, HashSet<Integer>> users = loadUsers();
        System.out.println(users.size());
        System.out.println(System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        HashMap<Integer, HashMap<Integer, Double>> similarity = getSimilarity(users);
        System.out.println(similarity.size());
        System.out.println(System.currentTimeMillis() - start);


        saveResults(similarity);
        System.out.println("DONE");
    }

    private static void saveResults(HashMap<Integer, HashMap<Integer, Double>> similarity) throws IOException {
        Writer output = new BufferedWriter(new FileWriter("result.txt"));
        for (Map.Entry<Integer, HashMap<Integer, Double>> sim : similarity.entrySet().stream().limit(100).collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue())).entrySet()) {
            if (sim.getValue().size() > 0) {
                String resultString = "User\t" + sim.getKey() + ":\n";

                for (Map.Entry<Integer, Double> u : sim.getValue().entrySet().stream().sorted(Map.Entry.<Integer, Double>comparingByValue().reversed()).limit(100).collect(Collectors.toList())) {
                    if (u.getValue() == 1) {
                        continue;
                    }

                    resultString += "\t" + u.getKey() + "\t" + new DecimalFormat("#.###").format(u.getValue()) + "\n";
                }
                output.append(resultString);
            }

        }
        output.close();
    }

    private static HashMap<Integer, HashMap<Integer, Double>> getSimilarity(Map<Integer, HashSet<Integer>> users) {
        HashMap<Integer, HashMap<Integer, Double>> similarity = new HashMap<>();
        users.entrySet().parallelStream().forEach(currentUser -> {
            synchronized (sync) {
                similarity.put(currentUser.getKey(), new HashMap<>());
            }

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
