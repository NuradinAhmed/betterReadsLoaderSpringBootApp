package builds.betterreaddataloader.author;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;



//for my modelAuthor - this is the repository - and just like JPA you give it two types: 
    ///two types: first type is entity class and second type is the Id -the type of id.
    //entity class here Author, and the type of the id which is string 
    //Now i have a repository class that i can use fetching data from cassandra as well as persisting data to cassandra. 
@Repository //Tells spring that this is acting repotiroy and dependcy injected and can be called on it. 
public interface AuthorRepository extends CassandraRepository<Author, String>{

	

    
}