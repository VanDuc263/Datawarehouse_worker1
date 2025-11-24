#!/bin/bash
# Vị trí project_dask
PROJECT_DIR=/root/project_dask

# Export PYTHONPATH để Python thấy workflow và utils
export PYTHONPATH=$PROJECT_DIR:$PYTHONPATH

# Active venv
source ~/venv/bin/activate

# Chạy Dask worker, connect tới scheduler
dask-worker tcp://192.168.1.100:8786 --name worker1
