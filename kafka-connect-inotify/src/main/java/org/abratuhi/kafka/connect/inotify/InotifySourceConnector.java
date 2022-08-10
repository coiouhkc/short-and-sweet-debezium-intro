package org.abratuhi.kafka.connect.inotify;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.List;
import java.util.Map;

public class InotifySourceConnector extends SourceConnector {
    public void start(Map<String, String> map) {

    }

    public Class<? extends Task> taskClass() {
        return InotifySourceTask.class;
    }

    public List<Map<String, String>> taskConfigs(int i) {
        return null;
    }

    public void stop() {

    }

    public ConfigDef config() {
        return null;
    }

    public String version() {
        return Version.get();
    }
}
