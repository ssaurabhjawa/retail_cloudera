import os
from util import get_spark_session
from read import from_jsonFiles


def main():
    BasicConfigurator.configure();
    env = os.environ.get('ENVIRON')
    src_dir=os.environ.get('SRC_DIR')
    src_file_pattern=f'{os.environ.get("SRC_FILE_PATTERN")}-*'
    src_file_format=os.environ.get('SRC_FILE_FORMAT')
    tgt_dir=os.environ.get('TGT_DIR')
    tgt_file_format=os.environ.get('TGT_FILE_FORMAT')
    spark=get_spark_session(env,'retail_poc')
    df=from_jsonFiles(spark,src_dir,src_file_format)
    df.show()

if __name__ == '__main__':
    main()