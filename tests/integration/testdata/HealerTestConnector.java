package e2e;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

// Testcontainers fixture only: files under /data/<test.id> control deliberate failures.
public class HealerTestConnector extends SourceConnector {
    private Map<String, String> properties;

    static Path directory(Map<String, String> properties) {
        return Path.of("/data", properties.get("test.id"));
    }

    static void recordStart(Path directory, String kind) {
        try {
            Files.createDirectories(directory);
            Files.writeString(directory.resolve(kind + "-starts"), System.currentTimeMillis() + "\n",
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException error) {
            throw new ConnectException("Could not record test start", error);
        }
    }

    @Override public String version() { return "1.0.0-test"; }
    @Override public void start(Map<String, String> properties) {
        this.properties = properties;
        Path directory = directory(properties);
        recordStart(directory, "connector");
        if (Files.exists(directory.resolve("fail-connector"))) {
            throw new ConnectException("Deliberate connector failure for Healer E2E test");
        }
    }
    @Override public Class<? extends Task> taskClass() { return ControlledTask.class; }
    @Override public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> tasks = new ArrayList<>();
        for (int task = 0; task < Math.min(maxTasks, Integer.parseInt(properties.getOrDefault("test.tasks", "1"))); task++) {
            Map<String, String> configuration = new HashMap<>(properties);
            configuration.put("test.task.id", Integer.toString(task));
            tasks.add(configuration);
        }
        return tasks;
    }
    @Override public void stop() { }
    @Override public ConfigDef config() {
        return new ConfigDef().define("test.id", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
            "Directory identifier used only by the end-to-end test")
            .define("test.tasks", ConfigDef.Type.INT, 1, ConfigDef.Importance.LOW, "Number of test tasks");
    }

    public static class ControlledTask extends SourceTask {
        private Path directory;
        private String id;
        private long sequence;
        private String taskId;
        @Override public String version() { return "1.0.0-test"; }
        @Override public void start(Map<String, String> properties) {
            directory = directory(properties);
            id = properties.get("test.id");
            taskId = properties.get("test.task.id");
            recordStart(directory, "task");
            recordStart(directory, "task-" + taskId);
            failIfRequested();
        }
        private void failIfRequested() {
            if (Files.exists(directory.resolve("fail-task")) || Files.exists(directory.resolve("fail-task-" + taskId))) {
                throw new ConnectException("Deliberate task failure for Healer E2E test");
            }
        }
        @Override public List<SourceRecord> poll() throws InterruptedException {
            Thread.sleep(250);
            failIfRequested();
            return List.of(new SourceRecord(Map.of("id", id), Map.of("sequence", ++sequence),
                "healer-e2e-records", Schema.STRING_SCHEMA, id, Schema.STRING_SCHEMA,
                "record-" + sequence));
        }
        @Override public void stop() { }
    }
}
