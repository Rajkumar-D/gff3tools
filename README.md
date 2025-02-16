# gff3tools

### Run the python code
spark-submit FtReader.py CDMU01000000 spark://hl-codon-38-03.ebi.ac.uk:39139 /homes/esa/spark/run/out


### Run using java 
java -Dspring.config.location=./run/application.properties -jar build/libs/gff3tools.jar  CTED01000000
/hps/software/users/tburdett/ena/esa/jvm/openjdk-17.0.2_linux-x64_bin/jdk-17.0.2/bin/java -Dspring.config.location=./application.properties -jar gff3tools.jar  CTED01000000