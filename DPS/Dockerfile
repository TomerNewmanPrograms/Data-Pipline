FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements.txt file to the working directory
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your app's source code to the working directory
COPY . .

# Set the command to run your FastAPI app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]