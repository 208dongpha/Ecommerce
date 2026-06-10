FROM apache/superset:latest

USER root

RUN which python || true
RUN python --version || true
RUN find / -name python 2>/dev/null | head -20 || true

USER superset