FROM python:3.10

EXPOSE 8080
WORKDIR /app

COPY . ./

RUN pip install -r requirements.txt

ENV PORT=8080
ENV STREAMLIT_LOG_LEVEL=debug

ENTRYPOINT ["streamlit", "run", "streamlit_app.py", "--server.port=8080", "--server.address=0.0.0.0"]