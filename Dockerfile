FROM python:3.11-slim
WORKDIR /app
RUN useradd -m botuser
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
USER botuser
CMD ["python", "-u", "bot.py"]
