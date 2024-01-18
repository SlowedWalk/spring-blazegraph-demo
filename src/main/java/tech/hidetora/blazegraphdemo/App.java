package tech.hidetora.blazegraphdemo;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import lombok.extern.slf4j.Slf4j;
import org.openrdf.OpenRDFException;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;

import java.io.*;
import java.util.Properties;

/**
 * @author hidetora
 * @version 1.0.0
 * @since 2022/04/18
 */

@Slf4j
public class App {

    public static void main(String[] args) throws IOException, OpenRDFException {

        // load journal properties from resources
        final Properties props = loadProperties("/blazegraph.properties");

        // instantiate a sail
        final BigdataSail sail = new BigdataSail(props);
        final Repository repo = new BigdataSailRepository(sail);

        try{
            repo.initialize();

            /*
             * Load data from resources
             * src/main/resources/data.n3
             */

            loadDataFromResources(repo, "/data.n3", "");

            final String query = "select * {<http://blazegraph.com/blazegraph> ?p ?o}";
            final TupleQueryResult result = executeSelectQuery(repo, query, QueryLanguage.SPARQL);

            try {
                while(result.hasNext()){

                    final BindingSet bs = result.next();
                    log.info("Binding Set :: {}", bs.toString());

                }
            } finally {
                result.close();
            }
        } finally {
            repo.shutDown();
        }
    }

    /*
     * Load a Properties object from a file.
     */
    public static Properties loadProperties(final String resource) throws IOException {
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
                                                      final QueryLanguage ql) throws OpenRDFException {

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
