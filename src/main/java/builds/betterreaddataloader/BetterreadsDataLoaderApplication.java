package builds.betterreaddataloader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.json.JSONArray;
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
import builds.betterreaddataloader.book.Book;
import builds.betterreaddataloader.book.BookRepository;



@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class) //Enable it here and give the class name to laod the configuration from it here and run
public class BetterreadsDataLoaderApplication {


	@Autowired AuthorRepository authorRepository; //local variable and then autowired for author repo


	@Autowired BookRepository bookRepository;   //local varialbe and then autowired for book repo


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

			lines.limit(10000).forEach(line -> { // i am going to get each line - am going to test and good way is to put
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
		Path paths = Paths.get(worksDumpLocation); // I have the path of the worksDumpLoaction


		//This will be used for formatting the dateformatter parser Creates a formatter using the specified pattern.
		//This method will create a formatter based on a simple pattern of letters and symbols as described in the class documentation. 
			//For example, d MMM uuuu will format 2011-12-03 as '3 Dec 2011'.
		DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"); //then assign to dateFormat and pass it to anywhere am parsing the date

		// I am going to get each line of the path given above as stream of lines
		try (Stream<String> lines = Files.lines(paths)) { // read all lines from a file as stream.

			// I am going to get each line - iterate over
			lines.limit(200).forEach(line -> { // Read and parse the lines

				// Read and parse the line
				String jsonString = line.substring(line.indexOf("{")); // Returns the index within this string of the
																		// first occurrence of
																		// the specified { curly braces to get the json
																		// part of the data

				// We then convert the string Above to JSON OJbect using JSON API.
				try {

					JSONObject jsonObject = new JSONObject(jsonString);

					// Once i have as JSON Object of the intWorks(book info) I need to save it

					// Construct book object //am going to parse each line above file and then put
					// into the book object
					Book book = new Book();
					book.setId(jsonObject.getString("key").replace("/works/", ""));

					book.setName(jsonObject.optString("title"));

					// first i need to get the description which is json object and i need value
					// from there on. ex. "description": {"type": "/type/text", "value": "Adam and
					// his"}
					JSONObject descriptionObj = jsonObject.optJSONObject("descriptioin"); // we are getting the JSON
																							// Ojbect of description
					if (descriptionObj != null) { // and then here we checkig if it exist and if it does
						book.setDescription(descriptionObj.optString("value")); // then we are getting the string value
					}

					JSONObject publishedObj = jsonObject.optJSONObject("created"); // and here we are getting the actaul
																					// JSON Object created
					if (publishedObj != null) { // checking if it exist and if it does
						String dateStr = publishedObj.optString("value"); // then we are getting the value from the json
																			// as string
						book.setPublishedDate(LocalDate.parse(dateStr, dateFormat)); // and then we are parsing it here - converting
																			// it to localdate and setting it to
																			// publishedate.
					}

					JSONArray coversJSONArr = jsonObject.optJSONArray("covers");
					if (coversJSONArr != null) {
						List<String> coverIds = new ArrayList<>(); // list of an arraylist as string . ex. coverIds:
																	// "covers": [4567, 67888, 34556] - as you can see
																	// its a list of string.An arrayList
						for (int i = 0; i < coversJSONArr.length(); i++) {

							// each string i get from the loop above am adding to the coverIds property
							// which in itself is arrayList.
							coverIds.add(coversJSONArr.getString(i)); // get json array using get string method. tekes
																		// an index give it back a string .
						}

						book.setCoverIds(coverIds);
					}

					JSONArray authorJsonArray = jsonObject.optJSONArray("authors");
					if (authorJsonArray != null) {
						List<String> authorIds = new ArrayList<>(); // creating here authorIds as an arrayList
						for (int i = 0; i < authorJsonArray.length(); i++) {

							// for each iteration i need to get the author object which is the i index of
							// the authorJsonarray
							String authorId = authorJsonArray.getJSONObject(i). // and finally the result put into field
																				// authroId of string.
							getJSONObject("author"). // then am gong to get the JSON object within it named author
							getString("key"). // and and then from there get the string key -
							replace("/authors/", ""); // to get the key we need to replace "/authors/" infront of it
														// with empty string/character and only get the key after it .

							authorIds.add(authorId); // so finally am adding the authorid string into the authorIds
														// array - stores as memorty and its defined above as an
														// arrayList so i can acess it
						}

						book.setAuthorIds(authorIds);

						// ------------------------------How do we get the author names -
						// we need to make a call to our author repository in cassandra and say hey here
						// is the authorId give me the corresponding authorname so that i can save into
						// the book repository
						List<String> authorNames = authorIds.stream().map(id -> authorRepository.findById(id))
								.map(optionalAuthor -> {
									if (!optionalAuthor.isPresent())
										return "Unknown Author"; // if it cannot find the authorName then return unknown
									return optionalAuthor.get().getName(); // otherwise get me the author name.
								}).collect(Collectors.toList());

						book.setAuthorNames(authorNames);

						// Persisit using Repository
						System.out.println("Saving book " + book.getName() + "...");
						bookRepository.save(book);


					}

				} catch (Exception e) {
					// handle the exception by printin it
					e.printStackTrace();
				}

			}

			);

		} catch (IOException e) {
			// hndle the exception - am just gonna print the exception
			e.printStackTrace();
		}

	}





	//create a method which runs when the application starts
	@PostConstruct
	public void start() { 
		//initAuthors method will be called in here
		//initAuthors(); //comment not to run again when pushed all data.
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
