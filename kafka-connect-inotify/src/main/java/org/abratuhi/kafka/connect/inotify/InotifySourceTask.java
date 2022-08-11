package org.abratuhi.kafka.connect.inotify;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.List;
import java.util.Map;

public class InotifySourceTask extends SourceTask {

    private String filename;

    @Override
    public String version() {
        return Version.get();
    }

    @Override
    public void start(Map<String, String> props) {
        System.out.println("InotifySourceTask#start");
        this.filename = props.get(InotifySourceConfig.SOURCE_FILE);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        System.out.println("InotifySourceTask#poll");
        return List.of(
                new SourceRecord(null, null, filename, null, filename)
        );
    }

    @Override
    public void stop() {
        System.out.println("InotifySourceTask#stop");
    }
}
