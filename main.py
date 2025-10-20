import os
from fastapi import FastAPI
import uvicorn

app = FastAPI(title="My API", description="A simple API deployed on Railway")

@app.get("/")
def read_root():
    return {"message": "Hello World!", "status": "API is running!"}

@app.get("/health")
def health_check():
    return {"status": "healthy"}

@app.get("/items/{item_id}")
def read_item(item_id: int, q: str = None):
    return {"item_id": item_id, "q": q}

if __name__ == "__main__":
    # This is the key fix: Use Railway's PORT environment variable
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
