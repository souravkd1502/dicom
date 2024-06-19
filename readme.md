# FastAPI File Upload Endpoint

This is a simple FastAPI application that provides an endpoint for uploading files via HTTP POST requests.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

- Python 3.10
- pip (Python package installer)

### Installation

1. Clone the repository:


2. Install the dependencies:


### Running the Application

Run the FastAPI application using the following command:


The application will start and listen for incoming requests on `http://localhost:8000`.

## Usage

### Uploading Files

To upload a file, send a POST request to the `/upload/` endpoint with the file attached as a form field named `file`. Here's an example using cURL:


Replace `/path/to/your/file.jpg` with the path to the file you want to upload.

## Endpoint

### POST /upload/

#### Request

- Method: POST
- URL: `/upload/`
- Body: Form-data with a file field named `file`

#### Response

- Status Code: 200 (OK)
- Body: JSON object containing details of the uploaded file, including filename, content type, and size in bytes

Example response:

```json
{
  "filename": "example.jpg",
  "content_type": "image/jpeg",
  "size": 12345
}
```

Replace `/path/to/your/file.jpg` with the path to the file you want to upload.

## Endpoint

### POST /upload/

#### Request

- Method: POST
- URL: `/upload/`
- Body: Form-data with a file field named `file`

#### Response

- Status Code: 200 (OK)
- Body: JSON object containing details of the uploaded file, including filename, content type, and size in bytes

Example response:

```json
{
  "filename": "example.jpg",
  "content_type": "image/jpeg",
  "size": 12345
}
```