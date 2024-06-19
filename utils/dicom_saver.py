"""
dicom_saver.py

A module to handle saving DICOM files and metadata to RDS and S3.

-   This module defines a class DicomSaver that provides methods to save DICOM files and metadata
    to an RDS instance and an S3 bucket.
    
-   This module also defines a class AWSClient that initializes rhe S3 and RDS clients and is 
    then inherited by DicomSaver.

Example usage:

    # Initialize DicomSaver with RDS instance and S3 bucket name
    dicom_saver = DicomSaver(
        s3_bucket=os.getenv("S3_BUCKET"),
        region_name=os.getenv("REGION_NAME"),
        access_key_id=os.getenv("ACCESS_KEY_ID"),
        secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
    )

    # Process and save DICOM file
    dicom_saver.process_and_save(
        dicom_bytes=contents,
        filename=file.filename,
    )
"""

import os
import boto3
import io
import pydicom
import threading
import psycopg2

from typing import List, Tuple, Dict

# Initialize logging
from utils.logger import create_logger
_logger = create_logger(logger_name="DicomSaver")


class AWSClient:
    """
    A class for creating AWS clients for accessing various AWS services.

    Args:
        region_name (str): The AWS region name where the services are to be accessed.
        access_key_id (str): The AWS access key ID for authentication.
        secret_access_key (str): The AWS secret access key for authentication.
        s3_client (boto3.client): Boto3 client for accessing Amazon S3 service.
        rds_client (boto3.client): Boto3 client for accessing Amazon RDS service.
    """
    def __init__(self, region_name, access_key_id, secret_access_key):
        self.region_name = region_name
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.s3_client = boto3.client(
            's3',
            region_name=self.region_name,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key
        )
        self.rds_client = boto3.client(
            'rds',
            region_name=self.region_name,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key
        )

class DicomSaver(AWSClient):
    """
    A class to handle saving DICOM files and metadata to RDS and S3.

    Args:
        s3_bucket (str): The name of the S3 bucket.
    """
    def __init__(self, s3_bucket: str, region_name: str, access_key_id: str, secret_access_key: str) -> None:
        super().__init__(region_name, access_key_id, secret_access_key)
        self.s3_bucket = s3_bucket


    def save_to_rds(self, dicom_metadata: dict, query: str) -> None:
        """
        Connects to the RDS instance and saves DICOM metadata.

        Args:
            dicom_metadata (dict): The DICOM metadata to be saved.
            query (str): The SQL query to save the DICOM metadata.
        """
        # Connect to RDS instance
        try:
            # Example: Connect to RDS
            conn = psycopg2.connect(
                database=os.getenv("DATABASE"), 
                user=os.getenv("USER"), 
                password=os.getenv("PASSWORD"), 
                host=os.getenv("HOST"), 
                port=os.getenv("PORT")
            )
            _logger.info("Connecting to local database")
            cursor = conn.cursor()
            
            # Execute SQL query to save metadata
            cursor.execute(query, dicom_metadata)
            
            # Commit the transaction
            conn.commit()
            
            _logger.info("DICOM metadata saved to RDS successfully.")
        except Exception as e:
            # Rollback in case of error
            conn.rollback()
            _logger.info(f"Error saving DICOM metadata to RDS: {e}")
        finally:
            # Close cursor and connection
            cursor.close()
            conn.close()


    def save_to_s3(self, dicom_bytes: bytes, filename: str) -> None:
        """
        Uploads DICOM bytes to the specified S3 bucket.

        Args:
            dicom_bytes (bytes): The DICOM file content as bytes.
            filename (str): The filename to be used in S3.
        """
        # Upload DICOM bytes to S3 bucket
        if ".dcm" not in filename:
            self.s3_client.put_object(
                Bucket=self.s3_bucket, 
                Key=filename + ".dcm", 
                Body=dicom_bytes
            )
        else:
            self.s3_client.put_object(
                Bucket=self.s3_bucket, 
                Key=filename, 
                Body=dicom_bytes
            )

    def process_and_save(self, dicom_bytes: bytes, filename: str) -> None:
        """
        Extracts DICOM metadata, saves it to RDS, and saves the DICOM file to S3.

        Args:
            dicom_bytes (bytes): The DICOM file content as bytes.
            filename (str): The filename to be used in S3.
        """
        # Extract DICOM metadata
        dicom_data = pydicom.dcmread(io.BytesIO(dicom_bytes))
        metadata = self.read_dicom_metadata(dicom_data)
        data = self.PrepareData(metadata)
        
        # Save metadata to RDS
        query = self.create_insert_query(
            table_name="public.dicom_metadata",
            values=metadata
        )
        
        # Concurrently save to RDS and S3
        rds_thread = threading.Thread(target=self.save_to_rds, args=(data, query))
        s3_thread = threading.Thread(target=self.save_to_s3, args=(dicom_bytes, filename))
        
        rds_thread.start()
        s3_thread.start()
        
        rds_thread.join()
        s3_thread.join()

    def read_dicom_metadata(self, dicom_data: pydicom.dataset.FileDataset) -> Dict:
        """
        Reads all the data in the DICOM metadata.

        Args:
            dicom_data (pydicom.dataset.FileDataset): The DICOM dataset.

        Returns:
            dict: A dictionary containing all the data in the DICOM metadata.
        """
        dicom_metadata = {}
        for element in dicom_data:
            # Check if the element has a value
            if element.value:
                # Convert the value to a string and store it in the dictionary
                dicom_metadata[element.name] = str(element.value)
        del dicom_metadata["Pixel Data"]
        return dicom_metadata
    
    def PrepareData(self, mergedData: Dict) -> List[Tuple[str]]:
        """
        Parses the Dicom metadata and to a tuple that could be inserted in the RDS.

        Args:
            mergedData (Dict): pydicom.dataset.FileDataset converted into a dict.

        Returns:
            List[Tuple[str]]: parsed Dicom metadata tuple
        """
        # Extract relevant information
        accession_number = mergedData.get('Accession Number', '')
        acquisition_date = mergedData.get('Acquisition Date', '')
        acquisition_matrix = mergedData.get('Acquisition Matrix', '')
        acquisition_number = mergedData.get('Acquisition Number', '')
        acquisition_time = mergedData.get('Acquisition Time', '')
        angio_flag = mergedData.get('Angio Flag', '')
        bits_allocated = mergedData.get('Bits Allocated', '')
        bits_stored = mergedData.get('Bits Stored', '')
        columns = mergedData.get('Columns', '')
        content_date = mergedData.get('Content Date', '')
        content_time = mergedData.get('Content Time', '')
        date_of_last_calibration = mergedData.get('Date of Last Calibration', '')
        deidentification_method = mergedData.get('De-identification Method', '')
        derivation_description = mergedData.get('Derivation Description', '')
        echo_time = mergedData.get('Echo Time', '')
        echo_train_length = mergedData.get('Echo Train Length', '')
        flip_angle = mergedData.get('Flip Angle', '')
        frame_of_reference_uid = mergedData.get('Frame of Reference UID', '')
        high_bit = mergedData.get('High Bit', '')
        image_comments = mergedData.get('Image Comments', '')
        image_orientation_patient = mergedData.get('Image Orientation (Patient)', '')
        image_position_patient = mergedData.get('Image Position (Patient)', '')
        image_type = mergedData.get('Image Type', '')
        imaged_nucleus = mergedData.get('Imaged Nucleus', '')
        imaging_frequency = mergedData.get('Imaging Frequency', '')
        in_plane_phase_encoding_direction = mergedData.get('In-plane Phase Encoding Direction', '')
        instance_creation_date = mergedData.get('Instance Creation Date', '')
        instance_creation_time = mergedData.get('Instance Creation Time', '')
        instance_creator_uid = mergedData.get('Instance Creator UID', '')
        instance_number = mergedData.get('Instance Number', '')
        largest_image_pixel_value = mergedData.get('Largest Image Pixel Value', '')
        lossy_image_compression = mergedData.get('Lossy Image Compression', '')
        lossy_image_compression_ratio = mergedData.get('Lossy Image Compression Ratio', '')
        mr_acquisition_type = mergedData.get('MR Acquisition Type', '')
        magnetic_field_strength = mergedData.get('Magnetic Field Strength', '')
        manufacturer = mergedData.get('Manufacturer', '')
        manufacturer_model_name = mergedData.get("Manufacturer's Model Name", '')
        modality = mergedData.get('Modality', '')
        number_of_averages = mergedData.get('Number of Averages', '')
        number_of_phase_encoding_steps = mergedData.get('Number of Phase Encoding Steps', '')
        patient_id = mergedData.get('Patient ID', '')
        patient_identity_removed = mergedData.get('Patient Identity Removed', '')
        patient_position = mergedData.get('Patient Position', '')
        patient_age = mergedData.get("Patient's Age", '')
        patient_name = mergedData.get("Patient's Name", '')
        patient_sex = mergedData.get("Patient's Sex", '')
        patient_weight = mergedData.get("Patient's Weight", '')
        percent_phase_field_of_view = mergedData.get('Percent Phase Field of View', '')
        percent_sampling = mergedData.get('Percent Sampling', '')
        performed_procedure_step_description = mergedData.get('Performed Procedure Step Description', '')
        performed_procedure_step_start_date = mergedData.get('Performed Procedure Step Start Date', '')
        performed_procedure_step_start_time = mergedData.get('Performed Procedure Step Start Time', '')
        photometric_interpretation = mergedData.get('Photometric Interpretation', '')
        pixel_bandwidth = mergedData.get('Pixel Bandwidth', '')
        pixel_spacing = mergedData.get('Pixel Spacing', '')
        procedure_code_sequence = mergedData.get('Procedure Code Sequence', '')
        repetition_time = mergedData.get('Repetition Time', '')
        requested_procedure_code_sequence = mergedData.get('Requested Procedure Code Sequence', '')
        requested_procedure_description = mergedData.get('Requested Procedure Description', '')
        rows = mergedData.get('Rows', '')
        sar = mergedData.get('SAR', '')
        sop_class_uid = mergedData.get('SOP Class UID', '')
        sop_instance_uid = mergedData.get('SOP Instance UID', '')
        samples_per_pixel = mergedData.get('Samples per Pixel', '')
        scanning_sequence = mergedData.get('Scanning Sequence', '')
        sequence_name = mergedData.get('Sequence Name', '')
        sequence_variant = mergedData.get('Sequence Variant', '')
        series_date = mergedData.get('Series Date', '')
        series_description = mergedData.get('Series Description', '')
        series_instance_uid = mergedData.get('Series Instance UID', '')
        series_number = mergedData.get('Series Number', '')
        series_time = mergedData.get('Series Time', '')
        slice_location = mergedData.get('Slice Location', '')
        slice_thickness = mergedData.get('Slice Thickness', '')
        software_versions = mergedData.get('Software Versions', '')
        spacing_between_slices = mergedData.get('Spacing Between Slices', '')
        study_comments = mergedData.get('Study Comments', '')
        study_date = mergedData.get('Study Date', '')
        study_description = mergedData.get('Study Description', '')
        study_id = mergedData.get('Study ID', '')
        study_instance_uid = mergedData.get('Study Instance UID', '')
        study_time = mergedData.get('Study Time', '')
        time_of_last_calibration = mergedData.get('Time of Last Calibration', '')
        timezone_offset_from_utc = mergedData.get('Timezone Offset From UTC', '')
        transmit_coil_name = mergedData.get('Transmit Coil Name', '')
        variable_flip_angle_flag = mergedData.get('Variable Flip Angle Flag', '')
        window_center = mergedData.get('Window Center', '')
        window_center_width_explanation = mergedData.get('Window Center & Width Explanation', '')
        window_width = mergedData.get('Window Width', '')
        
        invoice_tuple_data = (
            accession_number,
            acquisition_date,
            acquisition_matrix,
            acquisition_number,
            acquisition_time,
            angio_flag,
            bits_allocated,
            bits_stored,
            columns,
            content_date,
            content_time,
            date_of_last_calibration,
            deidentification_method,
            derivation_description,
            echo_time,
            echo_train_length,
            flip_angle,
            frame_of_reference_uid,
            high_bit,
            image_comments,
            image_orientation_patient,
            image_position_patient,
            image_type,
            imaged_nucleus,
            imaging_frequency,
            in_plane_phase_encoding_direction,
            instance_creation_date,
            instance_creation_time,
            instance_creator_uid,
            instance_number,
            largest_image_pixel_value,
            lossy_image_compression,
            lossy_image_compression_ratio,
            mr_acquisition_type,
            magnetic_field_strength,
            manufacturer,
            manufacturer_model_name,
            modality,
            number_of_averages,
            number_of_phase_encoding_steps,
            patient_id,
            patient_identity_removed,
            patient_position,
            patient_age,
            patient_name,
            patient_sex,
            patient_weight,
            percent_phase_field_of_view,
            percent_sampling,
            performed_procedure_step_description,
            performed_procedure_step_start_date,
            performed_procedure_step_start_time,
            photometric_interpretation,
            pixel_bandwidth,
            pixel_spacing,
            procedure_code_sequence,
            repetition_time,
            requested_procedure_code_sequence,
            requested_procedure_description,
            rows,
            sar,
            sop_class_uid,
            sop_instance_uid,
            samples_per_pixel,
            scanning_sequence,
            sequence_name,
            sequence_variant,
            series_date,
            series_description,
            series_instance_uid,
            series_number,
            series_time,
            slice_location,
            slice_thickness,
            software_versions,
            spacing_between_slices,
            study_comments,
            study_date,
            study_description,
            study_id,
            study_instance_uid,
            study_time,
            time_of_last_calibration,
            timezone_offset_from_utc,
            transmit_coil_name,
            variable_flip_angle_flag,
            window_center,
            window_center_width_explanation,
            window_width
        )

        return self.replace_empty_with_null(invoice_tuple_data)

    def replace_empty_with_null(self, input_tuple):
        """
        Replace empty strings in a tuple with NULL.

        Args:
            input_tuple (tuple): Input tuple to be processed.

        Returns:
            tuple: Tuple with empty strings replaced by NULL.
        """
        return tuple(None if value == '' else value for value in input_tuple)
    
    @staticmethod
    def create_insert_query(table_name: str, values: dict) -> str:
        """
        Creates an SQL insert query.

        Args:
            table_name (str): The name of the table to insert into.
            values (dict): A dictionary containing column names as keys and corresponding values to insert.

        Returns:
            str: The SQL insert query.
        """
        columns = ', '.join([f'"{col}"' for col in values.keys()])
        placeholders = ', '.join(['%s'] * len(values))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        return query
