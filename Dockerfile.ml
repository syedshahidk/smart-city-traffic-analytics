FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /workspace

# Keep the ML environment reproducible and separate from the streaming stack.
RUN python -m pip install --no-cache-dir \
    numpy==2.5.5 \
    pandas==3.1.1 \
    scipy==1.18.1 \
    scikit-learn==1.9.1 \
    pytest==9.2.0

CMD ["python", "-m", "pytest", "-q", "tests/test_forecasting.py"]
