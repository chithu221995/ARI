from fastapi import FastAPI

app = FastAPI(title="A.R.I. Engine")

@app.get("/")
def read_root():
    return {"message": "Hello from the A.R.I. Engine - An Asset Relevance Intelligence Solution for your portfolio tracking!"}

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/api/v1/ingest")
def start_ingestion():
    return {"status": "ingestion pipeline started"}