FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN addgroup --system crawler && adduser --system --ingroup crawler crawler

# ── Builder stage ─────────────────────────────────────────────────────────────
FROM base AS builder
WORKDIR /build
COPY pyproject.toml .
RUN pip install --upgrade pip && pip install --prefix=/install .

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM base AS runtime
WORKDIR /app
COPY --from=builder /install /usr/local
COPY . .
RUN chown -R crawler:crawler /app
USER crawler

EXPOSE 9090
CMD ["python", "-m", "crawler.scheduler"]
