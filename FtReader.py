from pyspark.sql import SparkSession
import sys
import os
import re

def readFeatures(input_file):
    spark = SparkSession.builder \
        .appName(input_file.split("/").pop()) \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .master(sparkMaster) \
        .getOrCreate()

    # Define the input file path
    #input_file = "/Users/rajkumar/Projects/gff3tools/out/CDMU01000000/set-CDMU01000000"  # Replace with your file path

    # Read the input file
    lines = spark.sparkContext.textFile(input_file)

    # Filter lines that start with "FT"
    ft_lines = lines.filter(lambda line: line.startswith("FT"))
    ft_count = ft_lines.filter(lambda line: re.match(r"^FT\s{3}\S+.*", line))

    # Calculate the total size in bytes of all the filtered lines
    feature_size = ft_lines.map(lambda line: len(line.encode('utf-8'))).reduce(lambda a, b: a + b)
    feature_count = ft_count.count()
    # Print the total size in bytes
    print("#######################################################")
    print(f"Total size of FT lines: {feature_size} bytes")
    print(f"Total feature count: {feature_count} ")
    print("#######################################################")
    if "set-" in input_file:
        annotation["contig_annotation_size_bytes"]=feature_size
        annotation["contig_feature_count"]=feature_count

    if "con-" in input_file:
            annotation["scaffold_annotation_size_bytes"]=feature_size
            annotation["scaffold_feature_count"]=feature_count
    # Stop the Spark session
    spark.stop()


def updateAnnotation():
    import cx_Oracle

    # Establish the connection to the Oracle database
    # Replace with your actual connection details
    dsn_tns = cx_Oracle.makedsn('ora-ena-pro-hl.ebi.ac.uk', '1531', service_name='ENAPRO')
    conn = cx_Oracle.connect(user='datalib', password='pegasus', dsn=dsn_tns)

    # Create a cursor to interact with the database
    cursor = conn.cursor()

    # Example: Update record in a table (replace with your actual SQL query)
    sql_update_query = f"""
    update DROP_ENA_6374_GFF3_2015 set
    contig_annotation_size_bytes = {annotation["contig_annotation_size_bytes"]},
    scaffold_annotation_size_bytes = {annotation.get("scaffold_annotation_size_bytes")},
    contig_feature_count= {annotation.get("contig_feature_count")},
    scaffold_feature_count= {annotation.get("scaffold_feature_count")}
    where primaryacc# = '{accession}'
    """
    print(annotation)
    print(sql_update_query)

    # Execute the update query with parameters
    cursor.execute(sql_update_query)

    # Commit the transaction
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()



accession = sys.argv[1]
sparkMaster = sys.argv[2]
outDir = sys.argv[3] +"/"+ accession +"/"

print("accession : "+accession)
print("sparkMaster : "+sparkMaster)
print("outDir : "+outDir)

files = [os.path.join(outDir, file) for file in os.listdir(outDir)]
# Filter files starting with "set-" or "con-"
filtered_files = [file for file in files if 'set-' in file or 'con-' in file]

annotation={"scaffold_annotation_size_bytes":0,"scaffold_feature_count":0}
for file in filtered_files:
    if 'set-' in file or 'con-' in file:
        readFeatures(file)

updateAnnotation()


