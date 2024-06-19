"""

"""

# Import dependencies
import io
import os
import pydicom
import psycopg2
import matplotlib.pyplot as plt
from fastapi import BackgroundTasks, Response

from .dicom_saver import AWSClient
from .logger import create_logger

_logger = create_logger("dicom_reader")

import pydicom
import io
import matplotlib.pyplot as plt
from fastapi import BackgroundTasks, Response

class DicomReader(AWSClient):
    """
    A class to handle saving DICOM files and metadata to RDS and S3.

    Args:
        s3_bucket (str): The name of the S3 bucket.
        region_name (str): AWS region name.
        access_key_id (str): AWS access key ID.
        secret_access_key (str): AWS secret access key.
    """
    def __init__(self, s3_bucket: str, region_name: str, access_key_id: str, secret_access_key: str) -> None:
        super().__init__(region_name, access_key_id, secret_access_key)
        self.s3_bucket = s3_bucket

    def create_presigned_url(self, s3_object_key: str) -> str:
        """
        Generate a presigned URL to share the DICOM file image from S3.

        Args:
            s3_object_key (str): The name of the DICOM file as S3 object key.
            expiration_time (int): Duration for how long S3 presigned URL will be valid.

        Returns:
            str: Presigned URL of the DICOM file converted to image.

        Raises:
            Exception: If there's an error generating the presigned URL.
        """        
        try:
            url = self.s3_client.generate_presigned_url(
                    ClientMethod='get_object',
                    Params={
                        'Bucket': self.s3_bucket, 
                        'Key': s3_object_key
                        },
                    ExpiresIn=3600,
            )
            return url
        except Exception as e:
            _logger.exception("Error generating preassigned URL: %s", e)
            raise
        
    def read_dicom_files(self, s3_object_key: str) -> pydicom.dataset.FileDataset:
        """
        Retrieve a DICOM file from an S3 bucket and return it as a Pydicom dataset.

        Args:
            s3_object_key (str): The key (path) of the DICOM file in the S3 bucket.

        Returns:
            pydicom.dataset.FileDataset: A Pydicom dataset representing the DICOM file.

        Raises:
            Exception: If there's an error while retrieving or parsing the DICOM file.
        """
        try:
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_object_key)
            dicom_bytes = response['Body'].read()
            dicom_dataset = pydicom.dcmread(io.BytesIO(dicom_bytes))
            return dicom_dataset
        except Exception as e:
            _logger.exception("Error getting DICOM file from S3: %s", e)
            raise

    def read_dicom_metadata(self, dicom_data: pydicom.dataset.FileDataset) -> dict:
        """
        Extract metadata from a DICOM dataset.

        Args:
            dicom_data (pydicom.dataset.FileDataset): The DICOM dataset.

        Returns:
            dict: A dictionary containing metadata from the DICOM dataset.
        """
        dicom_metadata = {}
        for element in dicom_data:
            if element.value:
                dicom_metadata[element.name] = str(element.value)
        del dicom_metadata["Pixel Data"]
        return dicom_metadata
    
    def create_dicom_plot(self, s3_object_key: str) -> io.BytesIO:
        """
        Create a plot from a DICOM image.

        Args:
            s3_object_key (str): The key of the DICOM file in the S3 bucket.

        Returns:
            io.BytesIO: A buffer containing the DICOM image plot.
        """
        dicom_dataset = self.read_dicom_files(s3_object_key)
        try:
            plt.imshow(dicom_dataset.pixel_array, cmap=plt.cm.bone)
            plt.title("DICOM Image")
            img_buf = io.BytesIO()
            plt.savefig(img_buf, format='png')
            plt.close()
            return img_buf
        except Exception as e:
            _logger.error("Couldn't read DICOM image from %s", s3_object_key)
            raise
        
    def get_img(self, s3_object_key: str, background_tasks: BackgroundTasks) -> Response:
        """
        Get a DICOM image.

        Args:
            s3_object_key (str): The key of the DICOM file in the S3 bucket.
            background_tasks (BackgroundTasks): Background tasks to run asynchronously.

        Returns:
            Response: An HTTP response containing the DICOM image.
        """
        img_buf = self.create_dicom_plot(s3_object_key)
        buf_contents: bytes = img_buf.getvalue()
        background_tasks.add_task(img_buf.close)
        headers = {'Content-Disposition': 'inline; filename="out.png"'}
        return Response(buf_contents, headers=headers, media_type='image/png')



class RdsDataFetcher:
    """
    A class to fetch data from an RDS instance with dynamic filtering, sorting, and pagination.

    Args:
        db_name (str): The name of the database.
        db_user (str): The username to connect to the database.
        db_password (str): The password to connect to the database.
        db_host (str): The hostname of the database.
        db_port (str): The port of the database.

    """
    def __init__(
        self, 
        db_name: str = os.getenv("DATABASE"), 
        db_user: str = os.getenv("USER"), 
        db_password: str = os.getenv("PASSWORD"), 
        db_host: str = os.getenv("HOST"),  
        db_port: str = os.getenv("PORT"),
        ) -> None:
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port

    def build_query(
        self, 
        table_name: str, 
        filters: dict = None, 
        sort_by: str = None, 
        sort_order: str = 'asc', 
        page: int = 1, 
        page_size: int = 10
        ) -> str:
        """
        Builds the SQL query based on the provided parameters.

        Args:
            table_name (str): Table name for which query needs to be performed.
            filters (dict): A dictionary containing filter conditions.
            sort_by (str): The column to sort by.
            sort_order (str): The sort order ('asc' or 'desc').
            page (int): The page number for pagination (1-based index).
            page_size (int): The number of records per page.

        Returns:
            str: The constructed SQL query.
        """
        query = f"SELECT * FROM {table_name}"

        # Build SQL query with filters
        if filters:
            filter_conditions = []
            for key, value in filters.items():
                filter_conditions.append(f"{key} = %s")
            filter_clause = " AND ".join(filter_conditions)
            query += f" WHERE {filter_clause}"

        # Add sorting
        if sort_by:
            query += f" ORDER BY {sort_by} {sort_order}"

        # Add pagination
        if page and page_size:
            offset = (page - 1) * page_size
            query += f" OFFSET {offset} LIMIT {page_size}"

        return query

    def fetch_data(self, query: str) -> list:
        """
        Fetches data from the RDS instance based on the provided query.

        Args:
            query (str): The SQL query to fetch data from the database.

        Returns:
            list: A list of tuples containing the fetched data.
        """
        try:
            # Connect to RDS instance
            conn = psycopg2.connect(
                database=self.db_name, 
                user=self.db_user, 
                password=self.db_password, 
                host=self.db_host, 
                port=self.db_port
            )
            cursor = conn.cursor()

            # Execute SQL query
            cursor.execute(query)
            data = [dict((cursor.description[i][0], value) \
               for i, value in enumerate(row)) for row in cursor.fetchall()]

            return data
        except Exception as e:
            print(f"Error fetching data from RDS: {e}")
        finally:
            # Close cursor and connection
            cursor.close()
            conn.close()
