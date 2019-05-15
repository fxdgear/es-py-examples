FROM python:3
RUN pip install elasticsearch==7.0.1 click GitPython python-dateutil ipdb elasticsearch-dsl
WORKDIR /examples
COPY . .
ENTRYPOINT ["python", "example.py"]
CMD ["--help"]
