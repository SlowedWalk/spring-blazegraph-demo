package tech.hidetora.blazegraphdemo.service;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.RDFWriterFactory;
import org.eclipse.rdf4j.rio.turtle.TurtleWriterFactory;
import org.openrdf.OpenRDFException;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.springframework.stereotype.Service;
import tech.hidetora.blazegraphdemo.App;
import tech.hidetora.blazegraphdemo.entity.Student;

import java.io.*;
import java.util.*;

@Service
@Slf4j
@RequiredArgsConstructor
public class StudentService {
    private final ObjectMapper objectMapper;
    public String createStudent() throws IOException, OpenRDFException {

        // load journal properties from resources
        final Properties props = loadProperties("/blazegraph.properties");

        // instantiate a sail
        final BigdataSail sail = new BigdataSail(props);
        final Repository repo = new BigdataSailRepository(sail);

        try {
            repo.initialize();
            /*
             * Load data from resources
             * src/main/resources/data.n3
             */

            loadDataFromResources(repo, "/student.n3", "");
            // Create a new student
// Load existing RDF data
            Model existingData = loadExistingData();

            // Create a new student
            ModelBuilder modelBuilder = new ModelBuilder(existingData);

            Model newStudent =  modelBuilder.subject("http://example.org/NewStudent")
                    .add("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://example.org/Student")
                    .add("http://example.org/name", "New Student")
                    .add("http://example.org/email", "new.student@example.com")
                    .add("http://example.org/phone", "+1-123-456-7890")
                    .add("http://example.org/address", "789 Elm St, Villagetown")
                    .build();

            // Print the updated data
            System.out.println("Updated RDF Data:");
            existingData.forEach(statement -> System.out.println(statement.getObject().stringValue()));

            // Save the updated data to a file
            saveRDFToFile(existingData, "updated_data.ttl");

        } finally {
            repo.shutDown();
        }
        return "Student created successfully";
    }

    private static Model loadExistingData() {
        // This function simulates loading existing RDF data
        ModelBuilder modelBuilder = new ModelBuilder();
        modelBuilder.setNamespace("ex", "http://example.org/");
        modelBuilder.subject("ex:Student1")
                .add("rdf:type", "ex:Student")
                .add("ex:name", "John Doe")
                .add("ex:email", "john.doe@example.com")
                .add("ex:phone", "+1-123-456-7890")
                .add("ex:address", "123 Main St, Cityville");

        return modelBuilder.build();
    }

    private static void saveRDFToFile(Model model, String fileName) {
        try (OutputStream outputStream = new FileOutputStream(fileName)) {
            RDFWriterFactory writerFactory = new TurtleWriterFactory();
            RDFWriter rdfWriter = writerFactory.getWriter(outputStream);

            rdfWriter.startRDF();
            model.forEach(rdfWriter::handleStatement);
            rdfWriter.endRDF();

            System.out.println("RDF data saved to file: " + fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<Student> getAllStudent() throws IOException, OpenRDFException {

        // load journal properties from resources
        final Properties props = loadProperties("/blazegraph.properties");

        // instantiate a sail
        final BigdataSail sail = new BigdataSail(props);
        final Repository repo = new BigdataSailRepository(sail);

        try {
            repo.initialize();
            /*
             * Load data from resources
             * src/main/resources/data.n3
             */

            loadDataFromResources(repo, "/student.n3", "");

            String sparqlInsert = "PREFIX ex: <http://tech.hidetora/>"
                    + "SELECT ?student ?name ?email ?phone ?address"
                    + "WHERE {"
                    + "  ?student a ex:Student ;"
                    + "           ex:name ?name ;"
                    + "           ex:email ?email ;"
                    + "           ex:phone ?phone ;"
                    + "           ex:address ?address ."
                    + "}";

            final TupleQueryResult result = executeSelectQuery(repo, sparqlInsert, QueryLanguage.SPARQL);
            List<Student> student = new ArrayList<>();
            Map<Object, Object> map = new HashMap<>();
            try {
                while(result.hasNext()){
                    final BindingSet bs = result.next();
                    log.info("RESULT SET :: {}", bs);
                    map.put(bs.getBinding("name"), bs.getValue("name"));
                    map.put(bs.getBinding("email"), bs.getValue("email"));
                    map.put(bs.getBinding("phone"), bs.getValue("phone"));
                    map.put(bs.getBinding("address"), bs.getValue("address"));

                    // convert the map to Student object
                    student.add(objectMapper.convertValue(map, Student.class));
                    log.info("RESULT :: {}", map);
                }
            } finally {
                result.close();
            }
               return student;
        } finally {
            repo.shutDown();
        }
    }

    /*
     * Load a Properties object from a file.
     */
    public Properties loadProperties(final String resource) throws IOException {
        final Properties p = new Properties();
        final InputStream is = App.class
                .getResourceAsStream(resource);
        final Reader reader  = new InputStreamReader(new BufferedInputStream(is));
        try{
            p.load(reader);
        }finally{
            reader.close();
            is.close();
        }
        return p;
    }

    /*
     * Load data from resources into a repository.
     */
    public static void loadDataFromResources(final Repository repo, final String resource, final String baseURL)
            throws OpenRDFException, IOException {

        final RepositoryConnection cxn = repo.getConnection();

        try {
            cxn.begin();
            try {
                final InputStream is = App.class.getResourceAsStream(resource);
                if (is == null) {
                    throw new IOException("Could not locate resource: " + resource);
                }
                final Reader reader = new InputStreamReader(new BufferedInputStream(is));
                try {
                    cxn.add(reader, baseURL, RDFFormat.N3);
                } finally {
                    reader.close();
                }
                cxn.commit();
            } catch (OpenRDFException ex) {
                cxn.rollback();
                throw ex;
            }
        } finally {
            // close the repository connection
            cxn.close();
        }
    }

    /*
     * Execute sparql select query.
     */
    public static TupleQueryResult executeSelectQuery(final Repository repo, final String query,
                                                      final QueryLanguage ql) throws OpenRDFException  {

        RepositoryConnection cxn;
        if (repo instanceof BigdataSailRepository) {
            cxn = ((BigdataSailRepository) repo).getReadOnlyConnection();
        } else {
            cxn = repo.getConnection();
        }

        try {

            final TupleQuery tupleQuery = cxn.prepareTupleQuery(ql, query);
            tupleQuery.setIncludeInferred(true /* includeInferred */);
            return tupleQuery.evaluate();

        } finally {
            // close the repository connection
            cxn.close();
        }
    }
}
