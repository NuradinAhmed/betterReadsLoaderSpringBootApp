package builds.betterreaddataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import builds.betterreaddataloader.author.Author;
import builds.betterreaddataloader.author.AuthorRepository;



@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class) //Enable it here and give the class name to laod the configuration from it here and run
public class BetterreadsDataLoaderApplication {


	@Autowired AuthorRepository authorRepository;


	//creating @value annotation that will allow me to grab locatoin path data and pass it to these property variables in memeory 
	@Value("${datadump.location.author}") //here am using spring expression to get the values from the application properties location file and give it to here to the member variables
	private String authorDumpLocation;
	
	@Value("${datadump.location.works}")
	private String worksDumpLocation;



	public static void main(String[] args) {
		SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
	}



	//here is a method that initializes all the data for authors into the database and it will be called from the start method below 
	private void initAuthors() {
		// firt get the path of the authors
		Path path = Paths.get(authorDumpLocation);
		try (Stream<String> lines = Files.lines(path)) { // I am going to go each stream of lines in the path given from
															// authors file

			lines.limit(10).forEach(line -> { // i am going to get each line - am going to test and good way is to put
												// limit in a stream like 10 records
				// 1-Read and parse the line
				String jsonString = line.substring(line.indexOf("{")); // Returns the index within this string of the
																		// first occurrence of the specified { curly
																		// braces to get the json part of the data

				try {
					JSONObject jsonObject = new JSONObject(jsonString); // use json API to create jsonobject from it.
					
					// 2, Construct Author object
					Author author = new Author();
					author.setName(jsonObject.optString("name"));
					author.setPersonalName(jsonObject.optString("personal_name"));
					author.setId(jsonObject.optString("key").replace("/authors/", "")); // here i need to remove the
																						// /authors/OL100003 infornt of
																						// it just to get the key after
																						// it with empyt string

					// 3. Persisit using repository
					System.out.println("Saving author " + author.getName() + "..."); //a nice message to know whats going on before saving it. 
					authorRepository.save(author);

					
				} catch (JSONException e) {  // if its not able to parse any line - i basically dont want to break anything - if one line breaks i want to continue and not break/stop the whole stream running 
					e.printStackTrace();

				}

			});

		} catch (IOException e) {
			e.printStackTrace();
		}
	}





	private void initWorks() {
		Path path = Paths.get(worksDumpLocation);

	}



	//create a method which runs when the application starts
	@PostConstruct
	public void start() { 
		//initAuthors method will be called in here
		initAuthors();
		initWorks();
		
		/* testing below 
		//System.out.println("Application started");
		//Now i can create a new Author and persisit using repository and Am going to dependecy inject the repository above for the Author by autowiring 
			//and ask the repository to save this author - persist 
		Author author = new Author();
		author.setId("id");
		author.setName("name");
		author.setPersonalName("personalName");
		authorRepository.save(author);   //after creating the new author - we are persisiting/saving here. 

		*/

		System.out.println(authorDumpLocation);

	}


	//This bean exposes the cqlSessionBuilder Customer - basically using the astraxProperties.getSecureConnectBundle path and then creating new cql sessionbuilder 
		//its a way to securely manage and connect my instant cassandara db using the driver bundle specifiying the path of
	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}

}
