package org.abratuhi.kafka.connect.inotify;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class InotifySourceConfig extends AbstractConfig {

    public static final String SOURCE_DIR = "source.dir";

    public InotifySourceConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(SOURCE_DIR, ConfigDef.Type.STRING, "/tmp", ConfigDef.Importance.HIGH, "Directory to watch")
                ;
    }
}
