FROM python:3.12-slim@sha256:d657ab0ade19f404a6ccc883ab399540de667aff751748ce23c07330c5a89e64

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
    && apt-get install --yes --no-install-recommends openssh-client rsync \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 collector \
    && useradd --uid 10001 --gid collector --no-create-home --shell /usr/sbin/nologin collector

WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install --retries 10 --timeout 300 .

USER 10001:10001
ENTRYPOINT ["miry-data-collect"]
