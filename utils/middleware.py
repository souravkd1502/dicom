"""
middleware.py

This module defines various middleware functions and lists of middleware
that can be added to FastAPI applications to handle security, CORS, authentication,
validation, logging, and response compression.

It also includes custom authentication, validation, and logging middleware functions.

"""

from fastapi import HTTPException, Request

from .logger import create_logger
_logger = create_logger("middleware")


# Define custom validation middleware
async def validate_request(request: Request):
    """
    Validate incoming requests.

    Args:
        request (Request): The incoming request.

    Raises:
        HTTPException: If the request validation fails.

    Returns:
        None
    """
    # Example: Check if a certain header is present in the request
    api_key = request.headers.get('X-API-Key')
    if not api_key:
        raise HTTPException(status_code=400, detail="X-API-Key header is missing")

# Define custom logging middleware
async def log_request(request: Request):
    """
    Log incoming requests.

    Args:
        request (Request): The incoming request.

    Returns:
        None
    """
    # Example: Log the request method and path
    _logger.info(f"Request: {request.method} {request.url}")

    # Example: Log request body (be careful with large payloads)
    if await request.body():
        _logger.info(f"Request Body: {await request.body()}")

    # Example: Log request headers
    _logger.info("Request Headers:")
    for name, value in request.headers.items():
        _logger.info(f"{name}: {value}")
