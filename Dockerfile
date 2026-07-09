FROM python:3.13-slim

WORKDIR /app

COPY listener.py /app/listener.py

# No third-party deps — stdlib only

EXPOSE 8090

CMD ["python", "-u", "/app/listener.py"]
