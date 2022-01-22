def from_jsonFiles(spark, data_dir, file_format):
    df = spark. \
        read. \
        format(file_format). \
        load(data_dir)
    return df

def from_files(spark, data_dir, file_format):
    df = spark. \
        read. \
        format(file_format). \
        load(data_dir)
    return df