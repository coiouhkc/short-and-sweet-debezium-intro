package org.abratuhi.kafka.connect.inotify;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class InotifySourceTask extends SourceTask {

    private WatchService watchService;

    private final Queue<String> changes = new ConcurrentLinkedQueue<>();

    @Override
    public String version() {
        return Version.get();
    }

    @Override
    public void start(Map<String, String> props) {
        //System.out.println("InotifySourceTask#start");
        Path baseDir = Path.of(props.get(InotifySourceConfig.SOURCE_DIR));

        new Thread(() -> {
            try {
                this.watchService = FileSystems.getDefault().newWatchService();

                baseDir.register(watchService,
                        StandardWatchEventKinds.ENTRY_CREATE,
                        StandardWatchEventKinds.ENTRY_MODIFY,
                        StandardWatchEventKinds.ENTRY_DELETE
                );

                // see https://www.baeldung.com/java-nio2-watchservice
                WatchKey key;
                while ((key = watchService.take()) != null) {
                    for (WatchEvent<?> event : key.pollEvents()) {
                        System.out.println(
                                "Event kind:" + event.kind()
                                        + ". File affected: " + event.context() + ".");

                        changes.add(event.context().toString());
                    }
                    key.reset();
                }
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        //System.out.println("InotifySourceTask#start completed");
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        //System.out.println("InotifySourceTask#poll");
        List<SourceRecord> result = new ArrayList<>();
        while(!changes.isEmpty()) {
            String filename = changes.poll();
            result.add(new SourceRecord(null, null, filename, null, filename));
        }
        return result;
    }

    @Override
    public void stop() {
        //System.out.println("InotifySourceTask#stop");

        try {
            this.watchService.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
