from shipment.entity.config_entity import DataIngestionConfig
from shipment.entity.artifact_entity import DataIngestionArtifact

from shipment.logger import logging
from shipment.exception import ShipmentException
import os,sys

from urllib.request import urlretrieve
from sklearn.model_selection import train_test_split
import pandas as pd

class DataIngestion:

    def __init__(self,data_ingestion_config:DataIngestionConfig) -> None:
        try:

            logging.info(f"{'='*20}Data Ingestion log started.{'='*20} ")
            self.data_ingestion_config = data_ingestion_config
            
        except Exception as e:
            raise ShipmentException(e,sys) from e

    def download_shipment_data(self) -> str:
        try:
            # getting download url 
            data_download_url = self.data_ingestion_config.dataset_download_url

            # creating directory
            raw_data_dir = self.data_ingestion_config.raw_data_dir

            if os.path.exists(raw_data_dir):
                os.remove(raw_data_dir)

            os.makedirs(raw_data_dir,exist_ok=True)

            # download file
            raw_data_file_path = os.path.join(self.data_ingestion_config.raw_data_dir,
                                            os.path.basename(data_download_url))

            logging.info(f"Downloading file from :[{data_download_url}] into :[{raw_data_file_path}]")                            
            urlretrieve(data_download_url,raw_data_file_path)
            logging.info(f"File :[{raw_data_file_path}] has been downloaded successfully.")
            
        except Exception as e:
            raise ShipmentException(e,sys) from e

    def data_cleaner(self,df:pd.DataFrame) -> pd.DataFrame:
        try:
            target_column = "Freight Cost (USD)"
            drop_col = ["index","ID"]

            if target_column in df.columns:
                df[target_column] = df[target_column].apply(lambda x: pd.to_numeric(x,errors="coerce"))
                df.dropna(subset=[target_column],axis=0,inplace=True)
                df.reset_index(inplace=True)
            else:
                raise Exception(f"{target_column} NOT FOUND IN DATASET.")

            for d in drop_col:
                if d in df.columns:
                    df.drop(columns=[d],axis=1,inplace=True)

            return df
        except Exception as e:
            raise ShipmentException(e,sys) from e

    def split_data_as_train_test(self,) -> DataIngestionArtifact:
        try:
            raw_data_dir = self.data_ingestion_config.raw_data_dir
            
            shipment_file_name = os.listdir(raw_data_dir)[0]
            
            shipment_file_path = os.path.join(raw_data_dir,shipment_file_name)

            logging.info(f"Reading csv file: [{shipment_file_path}]")
            shipment_data_frame = pd.read_csv(shipment_file_path)

            logging.info(f"Data cleaning process started")
            shipment_data_frame = self.data_cleaner(df=shipment_data_frame)
            logging.info("Data cleaning is done")

            X_train, X_test, y_train, y_test = train_test_split(shipment_data_frame.drop(["Freight Cost (USD)"],axis=1),
                                                                shipment_data_frame["Freight Cost (USD)"],
                                                                test_size=0.2,
                                                                 random_state=42
                                                                 )
            train_data = X_train.join(y_train)
            test_data = X_test.join(y_test)

            train_file_path = os.path.join(self.data_ingestion_config.ingested_train_dir,
                                            shipment_file_name)

            test_file_path = os.path.join(self.data_ingestion_config.ingested_test_dir,
                                        shipment_file_name)

            if train_data is not None:
                os.makedirs(self.data_ingestion_config.ingested_train_dir,exist_ok=True)
                logging.info(f"Exporting train dataset to file: [{train_file_path}]")
                train_data.to_csv(train_file_path,index=False)
            
            if test_data is not None:
                os.makedirs(self.data_ingestion_config.ingested_test_dir,exist_ok=True)
                logging.info(f"Exporting test dataset to file: [{test_file_path}]")
                test_data.to_csv(test_file_path,index=False)

            data_ingestion_artifact = DataIngestionArtifact(train_file_path=train_file_path,
                                test_file_path=test_file_path,
                                is_ingested=True,
                                message=f"Data ingestion completed successfully."
                                )
            
            logging.info(f"Data Ingestion artifact:[{data_ingestion_artifact}]")
            return data_ingestion_artifact    

        except Exception as e:
            raise ShipmentException(e,sys) from e

    def initiate_data_ingestion(self)-> DataIngestionArtifact:
        try:
            # download the data
            self.download_shipment_data()

            # data split
            return self.split_data_as_train_test()
        except Exception as e:
            raise ShipmentException(e,sys) from e

    def __del__(self):
        logging.info(f"{'>>'*20}Data Ingestion log completed.{'<<'*20} \n\n")


