/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static io.debezium.connector.jdbc.dialect.snowflake.SnowflakeDatabaseDialect.CSV_NULL_LABEL;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.security.PrivateKey;
import java.security.Security;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.hibernate.jdbc.Work;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVWriter;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.util.Stopwatch;

import net.snowflake.client.jdbc.internal.org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import net.snowflake.client.jdbc.internal.org.bouncycastle.openssl.PEMParser;
import net.snowflake.client.jdbc.internal.org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import net.snowflake.client.jdbc.internal.org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import net.snowflake.client.jdbc.internal.org.bouncycastle.operator.InputDecryptorProvider;
import net.snowflake.client.jdbc.internal.org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;

/**
 * Effectively writes the batches using Hibernate {@link Work}
 *
 * @author Mario Fiore Vitale
 */
public class RecordWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecordWriter.class);
    private final SessionFactory sessionFactory;
    private final QueryBinderResolver queryBinderResolver;
    private final JdbcSinkConnectorConfig config;
    private final DatabaseDialect dialect;

    public RecordWriter(SessionFactory sessionFactory, QueryBinderResolver queryBinderResolver, JdbcSinkConnectorConfig config, DatabaseDialect dialect) {
        this.sessionFactory = sessionFactory;
        this.queryBinderResolver = queryBinderResolver;
        this.config = config;
        this.dialect = dialect;
    }

    private StatelessSession openSessionWithRetry() {
        final int maxRetries = 5;
        final long retryDelayMillis = 2000;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                return sessionFactory.openStatelessSession();
            }
            catch (Exception e) {
                LOGGER.error("Failed to open StatelessSession, attempt {} of {}", attempt, maxRetries, e);

                try {
                    Thread.sleep(retryDelayMillis);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        throw new RuntimeException("Failed to open StatelessSession after retries.");
    }

    private String getPrivateKeyPassphrase() {
        return "";
    }

    private PrivateKey getPrivateKey(String privateKeyStr)
            throws Exception {
        String keyString = privateKeyStr
                .replace("\\n", "\n")
                .trim();

        net.snowflake.client.jdbc.internal.org.bouncycastle.asn1.pkcs.PrivateKeyInfo privateKeyInfo = null;
        Security.addProvider(new BouncyCastleProvider());
        // Read an object from the private key file.
        PEMParser pemParser = new PEMParser(new StringReader(keyString));
        Object pemObject = pemParser.readObject();
        if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo) {
            // Handle the case where the private key is encrypted.
            PKCS8EncryptedPrivateKeyInfo encryptedPrivateKeyInfo = (PKCS8EncryptedPrivateKeyInfo) pemObject;
            String passphrase = getPrivateKeyPassphrase();
            InputDecryptorProvider pkcs8Prov = new JceOpenSSLPKCS8DecryptorProviderBuilder().build(passphrase.toCharArray());
            privateKeyInfo = net.snowflake.client.jdbc.internal.org.bouncycastle.asn1.pkcs.PrivateKeyInfo
                    .getInstance(encryptedPrivateKeyInfo.decryptPrivateKeyInfo(pkcs8Prov));
        }
        else if (pemObject instanceof net.snowflake.client.jdbc.internal.org.bouncycastle.asn1.pkcs.PrivateKeyInfo) {
            // Handle the case where the private key is unencrypted.
            privateKeyInfo = (PrivateKeyInfo) pemObject;
        }
        pemParser.close();
        JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME);
        return converter.getPrivateKey(net.snowflake.client.jdbc.internal.org.bouncycastle.asn1.pkcs.PrivateKeyInfo.getInstance(privateKeyInfo));
    }

    private void executeAllWithRawJdbc(List<String> sqlStatements) {
        String jdbcUrl = config.getConnectionUrl();
        String user = config.getConnectionUser();
        String privateKeyStr = config.getConnectionPrivateKey();
        String password = config.getConnectionPassword();

        try {
            Properties props = new Properties();
            props.put("user", user);

            if (privateKeyStr == null || privateKeyStr.isEmpty()) {
                props.put("password", password);
            }
            else {
                props.put("privateKey", getPrivateKey(privateKeyStr));
            }

            try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
                    Statement stmt = conn.createStatement()) {

                conn.setAutoCommit(false);

                for (String sql : sqlStatements) {
                    if (sql != null && !sql.trim().isEmpty()) {
                        LOGGER.info("Executing SQL: {}", sql);
                        stmt.execute(sql.trim());
                    }
                }

                conn.commit();

            }
            catch (Exception execErr) {
                LOGGER.error("Execution failed, attempting rollback", execErr);
                throw new RuntimeException("Failed to execute SQL statements via Snowflake", execErr);
            }
        }
        catch (Exception e) {
            LOGGER.error("Failed to connect to Snowflake", e);
            throw new RuntimeException("Failed to connect to Snowflake", e);
        }
    }

    public void writeCSV(List<SinkRecordDescriptor> records, List<String> sqlStatements, String csvFilePath) throws IOException {
        Stopwatch writeStopwatch = Stopwatch.reusable();
        writeRecordsToCsv(records, csvFilePath);
        writeStopwatch.start();

        executeAllWithRawJdbc(sqlStatements);

        writeStopwatch.stop();
        LOGGER.trace("[PERF] Total write execution time {}", writeStopwatch.durations());
    }

    public void write(List<SinkRecordDescriptor> records, String sqlStatement, Boolean isBulkDelete) {
        final int maxRetries = 3;
        final long retryDelayMillis = 2000;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            Stopwatch writeStopwatch = Stopwatch.reusable();
            writeStopwatch.start();

            try (StatelessSession session = openSessionWithRetry()) {
                final Transaction transaction = session.beginTransaction();

                try {
                    if (isBulkDelete) {
                        session.doWork(processBulkDelete(records, sqlStatement));
                    }
                    else {
                        session.doWork(processBatch(records, sqlStatement));
                    }
                    transaction.commit();
                }
                catch (Exception e) {
                    transaction.rollback();
                    throw e;
                }

                writeStopwatch.stop();
                LOGGER.trace("[PERF] Total write execution time {}", writeStopwatch.durations());
                return;

            }
            catch (Exception e) {
                LOGGER.warn("Attempt {} of {} failed to write records: {}", attempt, maxRetries, e.getMessage());

                if (attempt >= maxRetries) {
                    LOGGER.error("All retry attempts failed. Giving up.");
                    throw new RuntimeException("Failed to write records after retries", e);
                }

                try {
                    Thread.sleep(retryDelayMillis);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Retry interrupted", ie);
                }
            }
        }
    }

    private Work processBulkDelete(List<SinkRecordDescriptor> records, String sqlStatement) {

        return conn -> {

            try (PreparedStatement prepareStatement = conn.prepareStatement(sqlStatement)) {

                QueryBinder queryBinder = queryBinderResolver.resolve(prepareStatement);
                Stopwatch bindStopwatch = Stopwatch.reusable();
                bindStopwatch.start();

                int paramIndex = 1;

                for (SinkRecordDescriptor record : records) {
                    final Struct keySource = record.getKeyStruct(config.getPrimaryKeyMode());

                    for (String fieldName : record.getKeyFieldNames()) {
                        final SinkRecordDescriptor.FieldDescriptor field = record.getFields().get(fieldName);
                        Object value = keySource.getWithoutDefault(fieldName);

                        List<ValueBindDescriptor> boundValues = dialect.bindValue(field, paramIndex, value);
                        boundValues.forEach(queryBinder::bind);
                        paramIndex += boundValues.size();
                    }
                }

                bindStopwatch.stop();
                LOGGER.trace("[PERF] All records bind execution time {}", bindStopwatch.durations());

                Stopwatch executeStopwatch = Stopwatch.reusable();
                executeStopwatch.start();

                int updateCount = prepareStatement.executeUpdate(); // no executeBatch — just one delete

                executeStopwatch.stop();

                if (updateCount == Statement.EXECUTE_FAILED) {
                    throw new BatchUpdateException("Bulk delete execution failed.", new int[]{ updateCount });
                }

                LOGGER.trace("[PERF] Execute bulk delete execution time {}", executeStopwatch.durations());
            }
        };
    }

    private String formatForCsv(String fieldName, Object value, Schema schema) {
        String schemaName = schema.name();

        if ("io.debezium.time.Date".equals(schemaName)) {
            int days = (Integer) value;
            LocalDate date = LocalDate.ofEpochDay(days);
            return date.toString();
        }
        else if ("io.debezium.time.Timestamp".equals(schemaName)) {
            Instant instant = Instant.ofEpochMilli(((Number) value).longValue());
            return instant.atOffset(ZoneOffset.UTC)
                    .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }
        else if ("io.debezium.time.MicroTimestamp".equals(schemaName)) {
            long micros = ((Number) value).longValue();
            Instant instant = Instant.ofEpochMilli(micros / 1000);
            return instant.atOffset(ZoneOffset.UTC)
                    .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }
        else if ("io.debezium.time.Time".equals(schemaName)) {
            long millis = ((Number) value).longValue();
            LocalTime time = LocalTime.ofNanoOfDay(millis * 1_000_000);
            return time.toString();
        }

        if (value instanceof java.time.ZonedDateTime) {
            return ((ZonedDateTime) value).format(DateTimeFormatter.ISO_ZONED_DATE_TIME);
        }
        else if (value instanceof java.time.OffsetDateTime) {
            return ((OffsetDateTime) value).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }
        else if (value instanceof java.time.LocalDateTime) {
            return ((LocalDateTime) value).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        }
        else if (value instanceof java.time.LocalDate) {
            return value.toString();
        }
        else if (value instanceof java.util.Date) {
            return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(
                    ((Date) value).toInstant().atOffset(ZoneOffset.UTC));
        }

        return value.toString();
    }

    public File writeRecordsToCsv(List<SinkRecordDescriptor> records, String csvFilePath) throws IOException {
        File tempFile;
        tempFile = new File(csvFilePath);

        if (!tempFile.exists()) {
            tempFile.getParentFile().mkdirs(); // Ensure the parent directories exist
            tempFile.createNewFile(); // Actually creates the file
        }

        try (CSVWriter writer = new CSVWriter(new FileWriter(tempFile))) {
            if (records.isEmpty()) {
                return tempFile;
            }

            SinkRecordDescriptor last = records.get(records.size() - 1);
            List<String> fieldNames = new ArrayList<>();
            fieldNames.addAll(last.getKeyFieldNames());
            fieldNames.addAll(last.getNonKeyFieldNames());

            fieldNames.add("_CHARGER_EXTRACTED_AT");
            fieldNames.add("_CHARGER_RAW_ID");

            writer.writeNext(fieldNames.toArray(new String[0]));

            for (SinkRecordDescriptor record : records) {
                List<String> row = new ArrayList<>();

                final Struct keySource = record.getKeyStruct(config.getPrimaryKeyMode());
                for (String fieldName : record.getKeyFieldNames()) {
                    Object value = keySource.getWithoutDefault(fieldName);
                    row.add(value == null ? CSV_NULL_LABEL : value.toString());
                }

                for (String fieldName : record.getNonKeyFieldNames()) {
                    final Struct source = record.getAfterStruct();
                    Field field = source.schema().field(fieldName);
                    Schema schema = field.schema();
                    Object value = source.getWithoutDefault(fieldName);
                    row.add(value == null ? CSV_NULL_LABEL : formatForCsv(fieldName, value, schema));
                }

                row.add(Instant.now().toString());
                row.add(UUID.randomUUID().toString());

                writer.writeNext(row.toArray(new String[0]));
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to write CSV file", e);
        }

        return tempFile;
    }

    private Work processBatch(List<SinkRecordDescriptor> records, String sqlStatement) {

        return conn -> {

            try (PreparedStatement prepareStatement = conn.prepareStatement(sqlStatement)) {

                QueryBinder queryBinder = queryBinderResolver.resolve(prepareStatement);
                Stopwatch allbindStopwatch = Stopwatch.reusable();
                allbindStopwatch.start();
                for (SinkRecordDescriptor sinkRecordDescriptor : records) {

                    Stopwatch singlebindStopwatch = Stopwatch.reusable();
                    singlebindStopwatch.start();
                    bindValues(sinkRecordDescriptor, queryBinder, false);
                    singlebindStopwatch.stop();

                    Stopwatch addBatchStopwatch = Stopwatch.reusable();
                    addBatchStopwatch.start();
                    prepareStatement.addBatch();
                    addBatchStopwatch.stop();

                    LOGGER.trace("[PERF] Bind single record execution time {}", singlebindStopwatch.durations());
                    LOGGER.trace("[PERF] Add batch execution time {}", addBatchStopwatch.durations());
                }
                allbindStopwatch.stop();
                LOGGER.trace("[PERF] All records bind execution time {}", allbindStopwatch.durations());

                Stopwatch executeStopwatch = Stopwatch.reusable();
                executeStopwatch.start();
                int[] batchResult = prepareStatement.executeBatch();
                executeStopwatch.stop();
                for (int updateCount : batchResult) {
                    if (updateCount == Statement.EXECUTE_FAILED) {
                        throw new BatchUpdateException("Execution failed for part of the batch", batchResult);
                    }
                }
                LOGGER.trace("[PERF] Execute batch execution time {}", executeStopwatch.durations());
            }
        };
    }

    private void bindValues(SinkRecordDescriptor sinkRecordDescriptor, QueryBinder queryBinder, Boolean bindKeyValuesOnly) {

        int index;
        if (sinkRecordDescriptor.isDelete() || bindKeyValuesOnly) {
            bindKeyValuesToQuery(sinkRecordDescriptor, queryBinder, 1);
            return;
        }

        switch (config.getInsertMode()) {
            case INSERT:
            case UPSERT:
                index = bindKeyValuesToQuery(sinkRecordDescriptor, queryBinder, 1);
                bindNonKeyValuesToQuery(sinkRecordDescriptor, queryBinder, index);
                break;
            case UPDATE:
                index = bindNonKeyValuesToQuery(sinkRecordDescriptor, queryBinder, 1);
                bindKeyValuesToQuery(sinkRecordDescriptor, queryBinder, index);
                break;
        }
    }

    private int bindKeyValuesToQuery(SinkRecordDescriptor record, QueryBinder query, int index) {

        if (Objects.requireNonNull(config.getPrimaryKeyMode()) == JdbcSinkConnectorConfig.PrimaryKeyMode.KAFKA) {
            query.bind(new ValueBindDescriptor(index++, record.getTopicName()));
            query.bind(new ValueBindDescriptor(index++, record.getPartition()));
            query.bind(new ValueBindDescriptor(index++, record.getOffset()));
        }
        else {
            final Struct keySource = record.getKeyStruct(config.getPrimaryKeyMode());
            if (keySource != null) {
                index = bindFieldValuesToQuery(record, query, index, keySource, record.getKeyFieldNames());
            }
        }
        return index;
    }

    private int bindNonKeyValuesToQuery(SinkRecordDescriptor record, QueryBinder query, int index) {
        return bindFieldValuesToQuery(record, query, index, record.getAfterStruct(), record.getNonKeyFieldNames());
    }

    private int bindFieldValuesToQuery(SinkRecordDescriptor record, QueryBinder query, int index, Struct source, List<String> fields) {

        for (String fieldName : fields) {
            final SinkRecordDescriptor.FieldDescriptor field = record.getFields().get(fieldName);

            Object value;
            value = source.getWithoutDefault(fieldName);

            LOGGER.debug("Binding field '{}' with value '{}' at index '{}'", fieldName, value, index);

            List<ValueBindDescriptor> boundValues = dialect.bindValue(field, index, value);

            boundValues.forEach(query::bind);
            index += boundValues.size();
        }
        return index;
    }
}
