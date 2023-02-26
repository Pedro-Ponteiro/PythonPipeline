FROM python:3.9
# adding this to pass tests...
# if you want to develop using the container, build this image and run it afterwards

WORKDIR /dev
COPY . /dev/
RUN pip install --no-cache-dir -r requirements.txt \
    && pre-commit install

CMD ["bash"]
