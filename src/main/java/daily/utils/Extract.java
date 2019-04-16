package daily.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.Callable;


public class Extract implements Callable<Void> {
    private static Logger LOG = LoggerFactory.getLogger(Extract.class);

    @Option(names = {"-u", "--url"}, required = true, paramLabel = "url", description = "jdbc url")
    String url;

    @Option(names = {"-U", "--user"}, required = true, paramLabel = "user", description = "jdbc user")
    String username;

    @Option(names = {"-P", "--password"}, required = true, paramLabel = "password", description = "jdbc password")
    String password;

    @Option(names = {"-o", "--output"}, required = true, paramLabel = "output", description = "output file")
    String output;

    @Option(names = {"-q", "--query"}, required = true, paramLabel = "query", description = "query")
    String query;

    @Option(names="--fetch-size", description = "fetch size")
    Integer fetchSize = 1000;

    @Option(names="--report-freq", description = "log frequency")
    Integer logFreq = 10000;

    @Option(names="--limit", description = "limit records")
    Integer limit = null;

    public static void main(String[] args) {
        CommandLine.call(new Extract(), args);
    }

    public Void call() throws Exception {
        LOG.info("Starting extract from {}@{} to {}, \n query {}", username, url, output, query);
        try (Connection con = getConnection();
             Statement st = con.createStatement()) {

            LOG.info("Connected to db");
            st.setFetchSize(fetchSize);

            Path path = Paths.get(output);

            if (Files.exists(path)) {
                LOG.warn("File {} already exists, will be truncated", path);
            }

            try (BufferedWriter writer = Files.newBufferedWriter(path,
                                                                 StandardOpenOption.TRUNCATE_EXISTING)) {

                LOG.info("Opened file for writing");

                try (ResultSet rs = st.executeQuery(query)) {
                    LOG.info("Executed query");

                    ResultSetMetaData metaData = rs.getMetaData();
                    int colCount = metaData.getColumnCount();
                    writeMeta(writer, metaData);


                    int records = 0;
                    LocalDateTime start = LocalDateTime.now();

                    while(rs.next()) {
                        writeRow(writer, rs, colCount);

                        if (++records % logFreq == 0) {
                            LOG.info("So far exported {} records", records);
                        }

                        if (limit != null && records >= limit) {
                            LOG.info("Extracted requested {} records, stop", limit);
                            break;
                        }
                    }

                    LOG.info("Completed processing {} records in {}", records,
                             Duration.between(start, LocalDateTime.now()));
                }
            }
        }

        return null;
    }

    private Connection getConnection() throws SQLException, ClassNotFoundException {
        if (url.contains("oracle")) {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        }
        return DriverManager.getConnection(url, username, password);
    }

    private void writeRow(BufferedWriter writer, ResultSet rs, int colCount)
            throws SQLException, IOException {
        for (int c = 0; c < colCount; c++) {
            if (c > 0) writer.write(",");

            Object object = rs.getObject(c + 1);
            if (object != null) {
                writer.write(object.toString());
            }
        }
        writer.newLine();
    }

    private void writeMeta(BufferedWriter writer, ResultSetMetaData metaData)
            throws SQLException, IOException {
        for (int c = 0; c < metaData.getColumnCount(); c++) {
            if (c > 0) writer.write(",");
            writer.write(metaData.getColumnLabel(c + 1));
        }
        writer.newLine();
    }
}
