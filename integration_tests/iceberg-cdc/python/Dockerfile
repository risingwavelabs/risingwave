FROM python:3.12

RUN curl -sSL https://install.python-poetry.org | python3 -

WORKDIR /app

COPY script /app

RUN $HOME/.local/bin/poetry update

CMD $HOME/.local/bin/poetry run python init.py;sleep 40;$HOME/.local/bin/poetry run python check.py
