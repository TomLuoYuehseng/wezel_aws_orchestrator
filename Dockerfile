FROM python:3 

WORKDIR /orchestrator

COPY ./* . 

RUN python3 -m pip install --upgrade pip 

RUN python3 -m pip install --no-cache-dir -r requirements.txt 

CMD ["python", "index.py"]  