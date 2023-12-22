FROM python:3.8-slim-buster

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip3 install -r requirements.txt

COPY app.py .

EXPOSE 8501

CMD streamlit run --server.port 8501 --server.enableCORS false app.py