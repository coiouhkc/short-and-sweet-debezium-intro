package org.abratuhi.kafka.connect.inotify;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class InotifySourceConnector extends SourceConnector {


    private Path baseDir;

    public void start(Map<String, String> map) {
        this.baseDir = Path.of(map.get(InotifySourceConfig.SOURCE_DIR));
    }

    public Class<? extends Task> taskClass() {
        return InotifySourceTask.class;
    }

    public List<Map<String, String>> taskConfigs(int i) {
        return List.of(
                Map.of(InotifySourceConfig.SOURCE_DIR, baseDir.toString())
        );
    }

    public void stop() {
    }

    public ConfigDef config() {
        return InotifySourceConfig.config();
    }

    public String version() {
        return Version.get();
    }
}
