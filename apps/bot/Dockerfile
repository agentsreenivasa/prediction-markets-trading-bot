FROM python:3.11-slim AS builder
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM python:3.11-slim
RUN groupadd -r botuser && useradd -r -g botuser botuser
WORKDIR /app
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY . .
RUN chown -R botuser:botuser /app
USER botuser
HEALTHCHECK --interval=30s --timeout=10s --retries=3 CMD python -c "import sys; sys.exit(0)"
ENV PAPER_TRADING=true
CMD ["python", "main.py"]
