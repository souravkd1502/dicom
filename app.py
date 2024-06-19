"""
DICOM API

This module provides APIs for managing DICOM files, including uploading DICOM files,
retrieving metadata, generating presigned URLs, and creating plots of DICOM images.

Endpoints:
    - /: Test connection to the FastAPI server.
    - /upload/: Upload a DICOM file.
    - /patient/{id}: Get patient data by ID.
    - /presigned-url/{s3_object_key}: Generate a presigned URL for a DICOM file.
    - /metadata/{s3_object_key}: Retrieve metadata of a DICOM file.
    - /plot/{s3_object_key}: Generate and return a plot of a DICOM image.

"""

"""
# TODO:
1. create API to upload DICOM files and metadata. 
2. Create API to get DICOM metadata by patient name.
3. Add CORS and Middleware support
4. Create API to directly read metadata and Show image for a 
   DICOM file without saving file to S3.
"""

# Import dependencies
import os
from fastapi import (
    FastAPI, 
    File, 
    UploadFile, 
    Response, 
    Request, 
    BackgroundTasks,
    HTTPException,
)
from typing import Annotated

from utils.dicom_saver import DicomSaver
from utils.dicom_reader import DicomReader
from utils.logger import create_logger

from dotenv import load_dotenv
load_dotenv()

# Define FastAPI Application
app = FastAPI(swagger_ui_parameters={"syntaxHighlight.theme": "obsidian"})

# Setup logging
_logger = create_logger("api")

# Initialize DicomSaver instance
dicom_saver = DicomSaver(
    s3_bucket=os.getenv("S3_BUCKET"),
    region_name=os.getenv("REGION_NAME"),
    access_key_id=os.getenv("ACCESS_KEY_ID"),
    secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
)
_logger.info("Successfully created DicomSaver")

# Initialize DicomReader instance
dicom_reader = DicomReader(
    s3_bucket=os.getenv("S3_BUCKET"),
    region_name=os.getenv("REGION_NAME"),
    access_key_id=os.getenv("ACCESS_KEY_ID"),
    secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
)
_logger.info("Successfully created DicomReader")

# ---------------------------------------------------------------- Test Connection ----------------------------------------------------------------
# Define api endpoint to test connection
@app.get("/")
def test_connection() -> dict:
    """
    Test the connection to the FastAPI server.

    Raises:
        HTTPException: If there is an error during the connection test.

    Returns:
        dict: A dictionary indicating the success of the connection test.
    """
    try:
        return {"message": "Connection successful"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------- Upload DICOM File ----------------------------------------------------------------
# Define api endpoint to upload a Dicom file to the S3 and RDS 
@app.post("/upload/")
async def upload_file(file: Annotated[UploadFile, File()]) -> Response:
    """
    Upload a DICOM file and save it in S3 and store metadata to RDS.

    Args:
        file (UploadFile, optional): The DICOM file to upload. Defaults to File(...).

    Returns:
        Response: A response indicating the status of the file upload.
    """
    try:
        # Read the contents of the file
        contents = await file.read(1024*1024)
        # Process and save the DICOM file
        dicom_saver.process_and_save(
            dicom_bytes=contents,
            filename=file.filename,
        )
        return {"status": 200}
    except Exception as e:
        _logger.exception("Error processing file: %s", e)
        return {"status": 500, "message": str(e)}

# ---------------------------------------------------------------- Get Pateint Dicom Metadata ----------------------------------------------------------------
# Define api endpoint to fetch dicom metadata by patient id
@app.get("/patient/{id}")
async def get_patient_by_id(request: Request) -> Response:
    """
    Get patient data by ID.

    Args:
        request (Request): The request containing query parameters.

    Returns:
        Response: A response containing the patient data.
    """
    # Parse query parameters
    id = request.args.get('id')
    filters = request.args.get('filters')
    sort_by = request.args.get('sort_by')
    sort_order = request.args.get('sort_order')
    page = request.args.get('page', default=1, type=int)
    page_size = request.args.get('page_size', default=10, type=int)
    # Fetch patient data
    dicom_reader.fetch_data()
    return {"status": 200}


# ---------------------------------------------------------------- Get Presigned URL for Dicom file --------------------------------
# Define api endpoint to create a S3 presigned url by filename
@app.get("/presigned-url/{s3_object_key}")
async def get_presigned_url(s3_object_key: str):
    """
    Generate a presigned URL for the DICOM file.

    Args:
        s3_object_key (str): The S3 object key of the DICOM file.

    Returns:
        dict: A dictionary containing the presigned URL of the DICOM file.
    """
    presigned_url = dicom_reader.create_presigned_url(s3_object_key)
    return {"presigned_url": presigned_url}


# ---------------------------------------------------------------- Get metadata from File --------------------------------
# Define api endpoint to read metadata directly from a file
@app.get("/metadata/{s3_object_key}")
async def get_metadata(s3_object_key: str):
    """
    Retrieve metadata of the DICOM file.

    Args:
        s3_object_key (str): The S3 object key of the DICOM file.

    Returns:
        dict: Metadata of the DICOM file.
    """
    dicom_dataset = dicom_reader.read_dicom_files(s3_object_key)
    metadata = dicom_reader.read_dicom_metadata(dicom_dataset)
    return metadata


# ---------------------------------------------------------------- Create DICOM image from file --------------------------------
# Define api endpoint to read a DICOM from S3 and show an image of it.
@app.get("/plot/{s3_object_key}")
async def get_plot(s3_object_key: str, background_tasks: BackgroundTasks):
    """
    Generate and return plot of the DICOM image.

    Args:
        s3_object_key (str): The S3 object key of the DICOM file.
        background_tasks (BackgroundTasks): Background tasks to run asynchronously.

    Returns:
        Response: Plot of the DICOM image.
    """
    img_buf = dicom_reader.create_dicom_plot(s3_object_key)
    buf_contents: bytes = img_buf.getvalue()
    background_tasks.add_task(img_buf.close)
    headers = {'Content-Disposition': 'inline; filename="out.png"'}
    return Response(buf_contents, headers=headers, media_type='image/png')
    

