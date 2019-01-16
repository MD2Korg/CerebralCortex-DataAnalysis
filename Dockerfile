FROM md2korg/cerebralcortex-kernel:2.4.0

RUN mkdir -p /cc_conf /spark_app
COPY . /spark_app

RUN pip3 install -r /spark_app/requirements.txt

VOLUME /cc_data

WORKDIR /spark_app

RUN python3 setup.py bdist_egg

CMD ["sh","compute_features.sh", "20181101", "20200101"]
