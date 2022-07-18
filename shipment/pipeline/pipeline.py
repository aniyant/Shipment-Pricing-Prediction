from collections import namedtuple
from datetime import datetime
import uuid
from shipment.config.configuration import Configuration
from shipment.logger import logging, get_log_file_name
from shipment.exception import ShipmentException
from typing import List
import os,sys

from shipment.entity.artifact_entity import DataIngestionArtifact
from shipment.component.data_ingestion import DataIngestion
from shipment.constant import *

class Pipeline:

    def __init__(self,config:Configuration = Configuration()) -> None:
        try:
            self.config = config
        except Exception as e:
            raise ShipmentException(e,sys) from e

    def start_data_ingestion(self)-> DataIngestionArtifact:
        try:
            data_ingestion = DataIngestion(data_ingestion_config=self.config.get_data_ingestion_config())
            
            return data_ingestion.initiate_data_ingestion()

        except Exception as e:
            raise ShipmentException(e,sys) from e

    def run_pipeline(self)-> None:
        try:
            # data ingestion
            self.start_data_ingestion()

        except Exception as e:
            raise ShipmentException(e,sys) from e


