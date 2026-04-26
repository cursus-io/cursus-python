# FastAPI Example

Minimal FastAPI app with Cursus async producer.

## Prerequisites

```bash
pip install cursus-client fastapi uvicorn
```

## Run

```bash
uvicorn app:app --reload
```

## Usage

```bash
curl -X POST http://localhost:8000/publish \
  -H "Content-Type: application/json" \
  -d '{"message": "hello", "key": "order-1"}'
```
