/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.StringReader;
import java.security.PrivateKey;
import java.security.Security;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.hibernate.cfg.AvailableSettings;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Field;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.PrimaryKeyMode;
import io.debezium.doc.FixFor;

import net.snowflake.client.jdbc.internal.org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import net.snowflake.client.jdbc.internal.org.bouncycastle.openssl.PEMParser;
import net.snowflake.client.jdbc.internal.org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import net.snowflake.client.jdbc.internal.org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import net.snowflake.client.jdbc.internal.org.bouncycastle.operator.InputDecryptorProvider;
import net.snowflake.client.jdbc.internal.org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;

/**
 * Unit tests for the {@link JdbcSinkConnectorConfig} class.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class JdbcSinkConnectorConfigTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSinkConnectorConfigTest.class);

    @Test
    public void testMissingRequiredConfigurationPropertiesFail() {
        final Field connectionUrl = JdbcSinkConnectorConfig.CONNECTION_URL_FIELD;
        final Field connectionUserName = JdbcSinkConnectorConfig.CONNECTION_USER_FIELD;
        final Field connectionPassword = JdbcSinkConnectorConfig.CONNECTION_PASSWORD_FIELD;

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Collections.emptyMap());
        assertThat(config.validateAndRecord(List.of(connectionUrl, connectionUserName, connectionPassword), LOGGER::error)).isFalse();
    }

    @Test
    public void testNonDefaultInsertModeProperty() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, "upsert");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        assertThat(config.validateAndRecord(List.of(JdbcSinkConnectorConfig.INSERT_MODE_FIELD), LOGGER::error)).isFalse();
        assertThat(config.getInsertMode()).isEqualTo(JdbcSinkConnectorConfig.InsertMode.UPSERT);
    }

    @Test
    public void testNonDefaultDeleteEnabledPropertyWithPrimaryKeyModeNotRecordKey() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSinkConnectorConfig.DELETE_ENABLED, "true");
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, "record_value");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        assertThat(config.validateAndRecord(List.of(JdbcSinkConnectorConfig.DELETE_ENABLED_FIELD, JdbcSinkConnectorConfig.PRIMARY_KEY_MODE_FIELD), LOGGER::error))
                .isFalse();
    }

    @Test
    public void testNonDefaultDeleteEnabledPropertyWithPrimaryKeyModeRecordKey() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSinkConnectorConfig.DELETE_ENABLED, "true");
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, "record_key");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        assertThat(config.validateAndRecord(List.of(JdbcSinkConnectorConfig.DELETE_ENABLED_FIELD, JdbcSinkConnectorConfig.PRIMARY_KEY_MODE_FIELD), LOGGER::error))
                .isTrue();
        assertThat(config.isDeleteEnabled()).isTrue();
    }

    @Test
    public void testNonDefaultTableNameFormatProperty() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSinkConnectorConfig.TABLE_NAME_FORMAT, "e2e-${topic}");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        assertThat(config.validateAndRecord(List.of(JdbcSinkConnectorConfig.TABLE_NAME_FORMAT_FIELD), LOGGER::error)).isTrue();
        assertThat(config.getTableNameFormat()).isEqualTo("e2e-${topic}");
    }

    @Test
    public void testNonDefaultPrimaryKeyModeProperty() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, "record_value");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        assertThat(config.validateAndRecord(List.of(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE_FIELD), LOGGER::error)).isTrue();
        assertThat(config.getPrimaryKeyMode()).isEqualTo(PrimaryKeyMode.RECORD_VALUE);
    }

    @Test
    public void testNonDefaultPrimaryKeyFieldsProperty() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id,name");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        assertThat(config.validateAndRecord(List.of(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS_FIELD), LOGGER::error)).isTrue();
        assertThat(config.getPrimaryKeyFields()).contains("id", "name");
    }

    @Test
    public void testNonDefaultSqlSelverIdentityTableNamesProperty() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSinkConnectorConfig.SQLSERVER_IDENTITY_INSERT, "true");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        assertThat(config.validateAndRecord(List.of(JdbcSinkConnectorConfig.SQLSERVER_IDENTITY_INSERT_FIELD), LOGGER::error)).isTrue();
        assertThat(config.isSqlServerIdentityInsert()).isTrue();
    }

    @Test
    @FixFor("DBZ-7431")
    public void testOverrideHibernateConfigurationProperties() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSinkConnectorConfig.CONNECTION_PROVIDER, "io.debezium.AcmeConnectionProvider");
        properties.put(JdbcSinkConnectorConfig.CONNECTION_URL, "jdbc://url");
        properties.put(JdbcSinkConnectorConfig.CONNECTION_USER, "user");
        properties.put(JdbcSinkConnectorConfig.CONNECTION_PASSWORD, "pass");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final Properties ormProperties = config.getHibernateConfiguration().getProperties();
        assertThat(ormProperties).isNotNull();
        assertThat(ormProperties.get(AvailableSettings.CONNECTION_PROVIDER)).isEqualTo("io.debezium.AcmeConnectionProvider");
        assertThat(ormProperties.get(AvailableSettings.URL)).isEqualTo("jdbc://url");
        assertThat(ormProperties.get(AvailableSettings.USER)).isEqualTo("user");
        assertThat(ormProperties.get(AvailableSettings.PASS)).isEqualTo("pass");
    }

    private static String getPrivateKeyPassphrase() {
        return "";
    }

    private static PrivateKey getPrivateKey(String privateKeyStr)
            throws Exception {
        String keyString = privateKeyStr
                .replace("\\n", "\n") // fix escaped newlines (e.g. from JSON or .env)
                .trim();

        PrivateKeyInfo privateKeyInfo = null;
        Security.addProvider(new BouncyCastleProvider());
        // Read an object from the private key file.
        PEMParser pemParser = new PEMParser(new StringReader(keyString));
        Object pemObject = pemParser.readObject();
        if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo) {
            // Handle the case where the private key is encrypted.
            PKCS8EncryptedPrivateKeyInfo encryptedPrivateKeyInfo = (PKCS8EncryptedPrivateKeyInfo) pemObject;
            String passphrase = getPrivateKeyPassphrase();
            InputDecryptorProvider pkcs8Prov = new JceOpenSSLPKCS8DecryptorProviderBuilder().build(passphrase.toCharArray());
            privateKeyInfo = PrivateKeyInfo.getInstance(encryptedPrivateKeyInfo.decryptPrivateKeyInfo(pkcs8Prov));
        }
        else if (pemObject instanceof PrivateKeyInfo) {
            // Handle the case where the private key is unencrypted.
            privateKeyInfo = (PrivateKeyInfo) pemObject;
        }
        pemParser.close();
        JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME);
        return converter.getPrivateKey(net.snowflake.client.jdbc.internal.org.bouncycastle.asn1.pkcs.PrivateKeyInfo.getInstance(privateKeyInfo));
    }

    @Test
    public void testUnencryptedPrivateKey() throws Exception {
        String unencryptedKey = """
                """;

        PrivateKey key = getPrivateKey(unencryptedKey);
        assertNotNull(key);
        assertEquals("RSA", key.getAlgorithm()); // or EC, depending on key
    }

    // @Test
    // public void testNonDefaultSchemaEvolutionProperty() {
    // final Map<String, String> properties = new HashMap<>();
    // properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, "advanced");
    //
    // final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
    // assertThat(config.validateAndRecord(List.of(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION_FIELD), LOGGER::error)).isTrue();
    // assertThat(config.getSchemaEvolutionMode()).isEqualTo(SchemaEvolutionMode.ADVANCED);
    // }

    // @Test
    // public void testDataTypeMappingProperty() {
    // final Map<String, String> properties = new HashMap<>();
    // properties.put(JdbcSinkConnectorConfig.DATA_TYPE_MAPPING, "table.column:GEOMETRY,table.column2:INT");
    //
    // final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
    // assertThat(config.validateAndRecord(List.of(JdbcSinkConnectorConfig.DATA_TYPE_MAPPING_FIELD), LOGGER::error)).isTrue();
    // assertThat(config.getDataTypeMapping()).contains("table.column:GEOMETRY", "table.column2:INT");
    // }
}
