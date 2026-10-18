# Multi-stage Dockerfile for Raft consensus broker node

# Stage 1: test stage
FROM python:3.11-slim AS test
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir pytest
# Module import validation
RUN python -c "import raft, server, broker; print('Modules OK')"

# Stage 2: production runtime stage
FROM python:3.11-slim AS runtime
WORKDIR /app

# Copy application packages
COPY raft/ ./raft/
COPY server/ ./server/
COPY broker/ ./broker/
COPY client/ ./client/
COPY cmd/ ./cmd/
COPY scripts/ ./scripts/
COPY conftest.py ./

# Environment defaults
ENV PYTHONUNBUFFERED=1
ENV RAFT_DATA_DIR=/data
VOLUME ["/data"]

EXPOSE 8000
ENTRYPOINT ["python", "-m", "broker.broker"]
