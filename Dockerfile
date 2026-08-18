# syntax=docker/dockerfile:1
#
# one image for all three processes, they are the same codebase started with different
# commands so compose builds this once and gives each service its own `command:`

FROM node:22-slim AS dashboard

# has to mirror the repo layout, vite.config.ts writes to '../backend/static' relative to the
# project root so building from here lands the bundle where the runtime stage expects it
WORKDIR /app/dashboard

COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci

COPY dashboard/ ./
RUN npm run build


FROM python:3.12-slim AS runtime

# otherwise print() sits in a buffer and `docker compose logs -f` looks frozen for minutes
ENV PYTHONUNBUFFERED=1

# makes `from config import ...` resolve under `python -m`
WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py ./
COPY producer/ producer/
COPY consumer/ consumer/
COPY backend/ backend/

# must stay after COPY backend/, static is excluded from the build context so this is the only
# thing that fills it
COPY --from=dashboard /app/backend/static backend/static

# no CMD, there is no single main process here and each service brings its own command
