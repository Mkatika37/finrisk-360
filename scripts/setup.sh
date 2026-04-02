#!/bin/bash
echo "Setting up FinRisk 360..."
pip install -r requirements.txt
docker-compose up -d
echo "Setup complete! Visit http://localhost:8501"
