FROM python:3
RUN pip install elasticsearch==7.0.1 click GitPython python-dateutil ipdb elasticsearch-dsl
RUN git clone https://github.com/elastic/elasticsearch-py.git
WORKDIR /examples
COPY . .
ENTRYPOINT ["python", "example.py"]
CMD ["--help"]
