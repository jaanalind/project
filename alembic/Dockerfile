FROM python:3.12-slim AS public

ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH /usr/

COPY ./alembic .
RUN pip install --no-cache-dir -r requirements.txt

CMD alembic -c ./alembic.ini upgrade head
