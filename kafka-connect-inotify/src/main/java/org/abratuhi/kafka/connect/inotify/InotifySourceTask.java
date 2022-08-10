package org.abratuhi.kafka.connect.inotify;

import org.apache.kafka.connect.connector.Task;

import java.util.Map;

public class InotifySourceTask implements Task {
    public String version() {
        return Version.get();
    }

    public void start(Map<String, String> map) {

    }

    public void stop() {

    }
}
