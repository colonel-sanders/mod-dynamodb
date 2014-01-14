package io.colonelsanders.vertx.dynamodb;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import io.colonelsanders.vertx.util.BeanPropertyUtils;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;

import java.beans.PropertyDescriptor;
import java.io.File;
import java.io.IOException;

public class AwsConfigHelper {

    private JsonObject config;
    private Logger logger;

    public AwsConfigHelper(JsonObject config, Logger logger) {
        this.config = config;
        this.logger = logger;
    }

    /**
     * Supporting either (1) loading from a properties file, with the config setting awsCredentialsFile or (2) directly
     * specifying the credentials via the accessKey and secretKey config settings.
     */
    public AWSCredentials configureCredentials() {
        String propsFile = config.getString("awsCredentialsFile");
        if (propsFile != null) {
            try {
                return new PropertiesCredentials(new File(propsFile));
            } catch (IOException e) {
                logger.error("Failed to load AWS credentials from configured properties file " + propsFile, e);
            }
        }
        String accessKey = config.getString("accessKey");
        String secretKey = config.getString("secretKey");
        if (accessKey == null || secretKey == null) {
            logger.error("Both accessKey and secretKey are required configuration parameters");
        }
        return new BasicAWSCredentials(accessKey, secretKey);
    }

    /**
     * ClientConfiguration can vary by SDK version, so let's be dynamic here.
     */
    public ClientConfiguration configureClient() {
        ClientConfiguration clientConfig = new ClientConfiguration();
        for (PropertyDescriptor beanProp : BeanPropertyUtils.writableProperties(ClientConfiguration.class)) {
            String value = config.getString(beanProp.getName());
            if (value != null) {
                BeanPropertyUtils.applyProperty(clientConfig, beanProp, value);
            }
        }
        return clientConfig;
    }
}
