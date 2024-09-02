FROM python:3.12-slim

WORKDIR /usr/src/app

COPY . ./

RUN pip install --no-cache-dir -r requirements.txt

COPY add_data.py .

CMD ["python", "./add_data.py"]
