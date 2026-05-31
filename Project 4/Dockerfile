FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    # libgomp1: required by PyTorch OpenMP threading (model load fails without it)
    libgomp1 \
    # libgl1: required by Pillow for PNG decoding
    libgl1 \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/airflow/.cache/huggingface && \
    chown -R airflow: /opt/airflow/.cache

USER airflow

COPY requirements.txt /requirements.txt

# Install CPU-only torch first, before the rest of requirements.txt.
# Without the --index-url override, pip pulls the CUDA build (~5 GB).
# The CPU build is ~700 MB and sufficient — no GPU is available in the container.
# open-clip-torch and sentence-transformers are both compatible with CPU torch.
RUN pip install --no-cache-dir \
        torch==2.3.1 \
        torchvision==0.18.1 \
        --index-url https://download.pytorch.org/whl/cpu \
 && pip install --no-cache-dir -r /requirements.txt