FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["python", "main_aquainfra.py"]

# Example build command:
#today=$(date '+%Y%m%d')
#docker build . -t niva-hydrochem:${today}

