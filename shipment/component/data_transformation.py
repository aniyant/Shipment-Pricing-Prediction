from re import L
from shipment.exception import ShipmentException
from shipment.logger import logging
from shipment.entity.config_entity import DataTransformationConfig
from shipment.entity.artifact_entity import DataIngestionArtifact,DataValidationArtifact, \
    DataTransformationArtifact
from shipment.constant import *
from shipment.util.util import read_yaml_file,save_object,save_numpy_array_data,load_data

from sklearn import preprocessing
from sklearn.base import BaseEstimator,TransformerMixin
from sklearn.preprocessing import StandardScaler,OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer

import numpy as np
import pandas as pd
import os,sys


class FeatureGenerator(BaseEstimator, TransformerMixin):

    def __init__(self,
                scheduled_delivery_date_ix = 0,
                delivered_to_client_date_ix = 1,   
                columns = None):
        """
        FeatureGenerator Initialization
        Scheduled Delivery Date: columns of type datetime  in the dataset
        Delivered to Client Date: columns of type datetime  in the dataset

        Genrated Feature
        late_days_between_delivery_scheduled : subtraction of  Delivered to Client Date and Scheduled Delivery Date
        """
        try:
            #logging.info(type(columns))
            #logging.info(columns)
            self.columns = columns
            if columns is not None:
                scheduled_delivery_date_ix = self.columns.index(SCHEDULED_DELIVERY_DATE_KEY)
                delivered_to_client_date_ix = self.columns.index(DELIVERED_TO_CLIENT_DATE_KEY)

            self.scheduled_delivery_date_ix = scheduled_delivery_date_ix
            self.delivered_to_client_date_ix = delivered_to_client_date_ix 
            logging.info(f"created new feature out of two datetime columns as {LATE_DAYS_BETWEEN_SCHEDULED_DELIVERY_COLUMN_KEY}") 

        except Exception as e:
            raise ShipmentException(e, sys) from e

    def fit(self, X, y=None):
        return self

    def transform(self, X, y=None):
        try:
            # train and test file path
            df = pd.DataFrame(X)
        
            scheduled = df[self.scheduled_delivery_date_ix].apply(lambda x: pd.to_datetime(x,errors="coerce"))
            delivered = df[self.delivered_to_client_date_ix].apply(lambda x: pd.to_datetime(x,errors="coerce"))

            df[LATE_DAYS_BETWEEN_SCHEDULED_DELIVERY_COLUMN_KEY] = delivered - scheduled
            df[LATE_DAYS_BETWEEN_SCHEDULED_DELIVERY_COLUMN_KEY] = df[LATE_DAYS_BETWEEN_SCHEDULED_DELIVERY_COLUMN_KEY].apply(lambda x:str(x).split(" ")[0]).astype('float')
            
            generated_feature = np.c_[np.array(df[LATE_DAYS_BETWEEN_SCHEDULED_DELIVERY_COLUMN_KEY])]
        
            return generated_feature                                                                             
        except Exception as e:
            raise ShipmentException(e, sys) from e


class DataTransformation:

    def __init__(self, data_transformation_config: DataTransformationConfig,
                 data_ingestion_artifact: DataIngestionArtifact,
                 data_validation_artifact: DataValidationArtifact
                 ):
        try:
            logging.info(f"{'>>' * 30}Data Transformation log started.{'<<' * 30} ")
            self.data_transformation_config= data_transformation_config
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_artifact = data_validation_artifact

        except Exception as e:
            raise ShipmentException(e,sys) from e


    def get_data_transformer_object(self) -> ColumnTransformer:
        try:
            schema_file_path = self.data_validation_artifact.schema_file_path
            
            dataset_schema = read_yaml_file(file_path=schema_file_path)

            numerical_columns = dataset_schema[DATASET_NUMERICAL_COLUMNS_KEY]
            categorical_columns = dataset_schema[DATASET_CATEGORICAL_COLUMNS_KEY]
            datetime_columns = dataset_schema[DATASET_DATETIME_COLUMNS_KEYS]

            num_pipeline = Pipeline(steps=[
                ('imputer',SimpleImputer(strategy="median")),
                ('scaler',StandardScaler())
            ])

            cat_pipeline = Pipeline(steps=[
                ('imputer',SimpleImputer(strategy="most_frequent")),
                ('one_hot_encoder',OneHotEncoder()),
                ('scaler',StandardScaler(with_mean=False))
            ])

            datetime_pipeline = Pipeline(steps = [
                ('imputer',SimpleImputer(strategy="constant",fill_value=0)),
                ('feature_generator',FeatureGenerator(
                    columns = datetime_columns
                )),
                ('scaler',StandardScaler())
            ])

            logging.info(f"Categorical columns : {categorical_columns}")
            logging.info(f"Numerical columns : {numerical_columns}")
            logging.info(f"Datetime_columns  : {datetime_columns}")

            preprocessing = ColumnTransformer([
                ('num_pipeline',num_pipeline,numerical_columns),
                ('cat_pipeline',cat_pipeline,categorical_columns),
                ('datetime_pipeline',datetime_pipeline,datetime_columns)
            ])

            return preprocessing
        except Exception as e:
            raise ShipmentException(e,sys) from e


    def initiate_data_transformation(self) -> DataTransformationArtifact:
        try:
            logging.info(f"Obtaining preprocessing_object.")
            preprocessing_obj = self.get_data_transformer_object()

            logging.info(f"Obtaining training and test file path.")
            train_file_path = self.data_ingestion_artifact.train_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path


            schema_file_path = self.data_validation_artifact.schema_file_path

            logging.info(f"Loading training and test data as pandas dataframe.")
            train_df = load_data(file_path=train_file_path,schema_file_path=schema_file_path)

            test_df = load_data(file_path=test_file_path,schema_file_path=schema_file_path)

            schema = read_yaml_file(file_path=schema_file_path)

            target_column_name = schema[DATASET_TARGET_COLUMN_KEY]

            logging.info(f"Splitting input and target feature from training and testing dataframe")
            input_feature_train_df = train_df.drop(columns=[target_column_name],axis=1)
            target_feature_train_df = train_df[target_column_name]

            input_feature_test_df = test_df.drop(columns=[target_column_name],axis = 1)
            target_feature_test_df = test_df[target_column_name]

            logging.info(f"Applying preprocessing object on training and testing dataframe")
            input_feature_train_arr = preprocessing_obj.fit_transform(input_feature_train_df)
            input_feature_test_arr = preprocessing_obj.fit_transform(input_feature_test_df)
            logging.info("Preprocessing is done")
            
            train_arr = np.append(input_feature_train_arr,np.array(target_feature_train_df))
            test_arr = np.append(input_feature_test_arr,np.array(target_feature_test_df))

            transformed_train_dir = self.data_transformation_config.transformed_train_dir
            transformed_test_dir = self.data_transformation_config.transformed_test_dir

            train_file_name = os.path.basename(train_file_path).replace(".csv",".npz")
            test_file_name = os.path.basename(test_file_path).replace(".csv",".npz") 

            transformed_train_file_path = os.path.join(transformed_train_dir, train_file_name)
            transformed_test_file_path = os.path.join(transformed_test_dir, test_file_name)

            logging.info(f"Saving transformed training and testing array.")

            save_numpy_array_data(file_path=transformed_train_file_path,array = train_arr)
            save_numpy_array_data(file_path=transformed_test_file_path,array=test_arr)

            preprocessing_obj_file_path = self.data_transformation_config.preprocessed_object_file_path

            logging.info(f"Saving preprocessing object.")
            save_object(file_path=preprocessing_obj_file_path,obj=preprocessing_obj)

            data_transformation_artifact = DataTransformationArtifact(
                is_transformed=True,
                message = "Data transformation successfull.",
                transformed_train_file_path=transformed_train_file_path,
                transformed_test_file_path=transformed_test_file_path,
                preprocessed_object_file_path=preprocessing_obj_file_path
                )

            logging.info(f"Data transformation artifact: {data_transformation_artifact}")
            return data_transformation_artifact

        except Exception as e:
            raise ShipmentException(e,sys) from e

    
    def __del__(self):
        logging.info(f"{'>>'*30}Data Transformation log completed.{'<<'*30} \n\n")
