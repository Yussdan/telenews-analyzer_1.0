FROM ubuntu:22.04

# Install dependencies
RUN apt-get update && apt-get install -y \
    python3 python3-pip build-essential cmake git wget curl \
    libcurl4-openssl-dev libgomp1 libopenblas-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Create model directory (empty - will be mounted from host)
RUN mkdir -p /app/models

# Clone and build llama.cpp
RUN git clone https://github.com/ggerganov/llama.cpp.git && \
    cd llama.cpp && \
    mkdir -p build && \
    cd build && \
    cmake .. -DLLAMA_CURL=ON -DBUILD_SERVER=ON -DCMAKE_BUILD_TYPE=Release && \
    cmake --build . --config Release -j $(nproc --ignore=1)

# Install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application files
COPY server.py start.sh /app/
RUN chmod +x /app/start.sh

EXPOSE 8000 8080

CMD ["/app/start.sh"]
