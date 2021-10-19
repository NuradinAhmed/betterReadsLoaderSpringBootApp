package builds.betterreaddataloader;

import java.io.File;

import org.springframework.boot.context.properties.ConfigurationProperties;

//We are exposing the bundle connection driver that exist in the properties as Java Class here using the annotation
@ConfigurationProperties(prefix = "datastax.astra") 
public class DataStaxAstraProperties {

        //the property will be the actual bundle driver name as camelCase and since the secureConnectBundle is zip file it needs to be a type of file
        private File secureConnectBundle;

        //the getter and setters for the File type property 
        public File getSecureConnectBundle() {
            return secureConnectBundle;
        }

        public void setSecureConnectBundle(File secureConnectBundle) {
            this.secureConnectBundle = secureConnectBundle;
        }

        
    
}