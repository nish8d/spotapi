# One image for every Python service. The simulator uses it in M1; the real
# Spotify poller reuses it unchanged in M3.
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY events/ events/
COPY producer/ producer/
COPY simulator/ simulator/
COPY poller/ poller/

# Unbuffered so container logs appear immediately rather than in blocks.
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "simulator.main"]
