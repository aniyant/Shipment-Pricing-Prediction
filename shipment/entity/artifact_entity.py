from collections import namedtuple

DataIngestionArtifact = namedtuple("DataIngestionArtifact",["is_ingested","train_file_path","test_file_path","message"])