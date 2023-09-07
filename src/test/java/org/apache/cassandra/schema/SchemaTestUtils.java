package org.apache.cassandra.schema;

import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaTestUtils {
    public static final Logger LOGGER = LoggerFactory.getLogger(SchemaTestUtils.class);

    public static void announceNewKeyspace(KeyspaceMetadata keyspaceMetadata) {
        keyspaceMetadata.validate();
        if (Schema.instance.getKeyspaceMetadata(keyspaceMetadata.name) != null) {
            throw new AlreadyExistsException(keyspaceMetadata.name);
        }
        LOGGER.info("Creating keyspace:{}", keyspaceMetadata);
        Schema.instance.transform(schema -> schema.withAddedOrUpdated(keyspaceMetadata));
    }

    public static void announceNewTable(TableMetadata tableMetadata) {
        tableMetadata.validate();
        KeyspaceMetadata keyspaceMetadata = Schema.instance.getKeyspaceMetadata(tableMetadata.keyspace);
        if (keyspaceMetadata == null) {
            throw new ConfigurationException(String.format("Cannot add table to keyspace doesnot exist"));
        } else if (keyspaceMetadata.getTableOrViewNullable(tableMetadata.name) != null) {
            throw new AlreadyExistsException(tableMetadata.keyspace, tableMetadata.name);
        }
        LOGGER.info("Creating table : {}", tableMetadata);
        Schema.instance.transform(
                schema -> schema.withAddedOrUpdated(keyspaceMetadata
                        .withSwapped(keyspaceMetadata.tables.with(tableMetadata))));

    }

    public static void announceKeyspaceUpdate(KeyspaceMetadata keyspaceMetadata) {
        keyspaceMetadata.validate();

        KeyspaceMetadata old = Schema.instance.getKeyspaceMetadata(keyspaceMetadata.name);
        if (old == null) {
            throw new ConfigurationException(String.format("Cannot update not exisitnace keyspace: {}",
                    keyspaceMetadata.name));
        }
        LOGGER.info("Updating keyspace:{} from : {} to : {}", keyspaceMetadata.name, old, keyspaceMetadata);
        Schema.instance.transform(schema -> schema.withAddedOrUpdated(keyspaceMetadata));
    }

    public static void announceTableUdpate(TableMetadata tableMetadata) {
        tableMetadata.validate();

        TableMetadata exising = Schema.instance.getTableMetadata(tableMetadata.keyspace, tableMetadata.name);
        KeyspaceMetadata keyspaceMetadata = Schema.instance.getKeyspaceMetadata(tableMetadata.keyspace);
        if (exising == null) {
            throw new ConfigurationException(String.format("Table does not existing:{}", tableMetadata.name));
        }
        tableMetadata.validateCompatibility(exising);
        LOGGER.info("Updating table:{} from : {} to : {}", tableMetadata.name, exising, tableMetadata);
        Schema.instance.transform(
                schema -> schema.withAddedOrUpdated(keyspaceMetadata
                        .withSwapped(keyspaceMetadata.tables.with(tableMetadata))));
    }


}
