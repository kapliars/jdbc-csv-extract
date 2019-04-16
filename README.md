JDBC CSV extract CLI
~~~~~~~~~~~~~~~~~~~~


Simple packaging tool to extract query result through JDBC and dump to CSV file. Used to remotely 
extract data from relational database.

Today supports oracle and mysql but can be easily extended for any other
jdbc compatible databases.

TODO:
- add gzip compression
- add manual column type mapping (now uses metadata)
- add support for custom timestamp and date formatting
- return different exit statuses for different errors


Simple built by maven into self-containing jar :
`mvn clean package`

that can be later run as
`java -jar target/jdbc-csv-extract*.jar -u jdbc:mysql://localhost:3306/dev -U dev -P 111 -o extract.csv -q "SELECT * FROM MY_TABLE WHERE AUTHOR='me'" --fetch-size 1`

optionally one can extract only limited number of records with --limit option.

