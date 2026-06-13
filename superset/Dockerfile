FROM apache/superset:3.1.0

USER root
RUN pip install --no-cache-dir trino==0.337.0

# Copy requirements nếu có
# COPY requirements.txt /tmp/
# RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER superset