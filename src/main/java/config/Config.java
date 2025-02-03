package config;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

public class Config {
    private String name;
    private String description;
    private String master;
    private boolean holdThread;
    private TreeMap<String, Object> sparkConfig;
    private ArrayList<User> users;


    public static Config parseBase(String config_path) throws IOException {
        Gson gson = new Gson();
        String json_file = new String(Files.readAllBytes(Paths.get(config_path)));
        return gson.fromJson(json_file, Config.class);
    }

    public static ArrayList<User> parseUsers(String config_path) throws IOException {
        Gson gson = new Gson();
        String json_file = new String(Files.readAllBytes(Paths.get(config_path)));
        Type listType = new TypeToken<ArrayList<User>>(){}.getType();
        return gson.fromJson(json_file, listType);
    }

    public boolean isHoldThread() {
        return holdThread;
    }

    public void setHoldThread(boolean holdThread) {
        this.holdThread = holdThread;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public Map<String, Object> getSparkConfig() {
        return sparkConfig;
    }

    public void setSparkConfig(TreeMap<String, Object> sparkConfig) {
        this.sparkConfig = sparkConfig;
    }

    public ArrayList<User> getUsers() {
        return users;
    }

    public void setUsers(ArrayList<User> users) {
        this.users = users;
    }

    @Override
    public String toString() {
        return "config.Config{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", master='" + master + '\'' +
                ", sparkConfig=" + sparkConfig +
                ", users=" + users +
                '}';
    }
}
