FROM python:3.6-slim
COPY backend /app
WORKDIR /app
ENV GOOGLE_APPLICATION_CREDENTIALS /app/course-project-ii-suyanx-d234ab287080.json
RUN pip3 install -r requirements.txt
EXPOSE 5001
ENTRYPOINT ["python3"]
CMD ["index.py"]
