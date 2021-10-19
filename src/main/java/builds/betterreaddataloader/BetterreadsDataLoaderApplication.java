package builds.betterreaddataloader;

import java.nio.file.Path;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class) //Enable it here and give the class name to laod the configuration from it here and run
public class BetterreadsDataLoaderApplication {

	public static void main(String[] args) {
		SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
	}



	//This bean exposes the cqlSessionBuilder Customer - basically using the astraxProperties.getSecureConnectBundle path and then creating new cql sessionbuilder 
		//its a way to securely manage and connect my instant cassandara db using the driver bundle specifiying the path of it 
	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}

}
