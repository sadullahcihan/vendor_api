from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import requests
from kafka import KafkaProducer
import json

app = FastAPI()

class Product(BaseModel):
    id: int
    name: str
    description: str
    price: float
    photoUrl: str

VENDOR_APIS = [
    "http://localhost:8001/vendor1/products",
    "http://localhost:8002/vendor2/products",
    "http://localhost:8003/vendor3/products"
]

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# home page
@app.get("/")
async def read_root():
    return {"message": "Welcome to the Unified Product Catalog API!"}

# list
@app.get("/products", response_model=List[Product])
async def get_products():
    combined_products = []
    
    for vendor_url in VENDOR_APIS:
        try:
            response = requests.get(vendor_url)
            response.raise_for_status()
            vendor_products = response.json()

            for vp in vendor_products:
                # combined
                product = Product(
                    id=vp.get("id", 0),
                    name=vp.get("name", "Unnamed Product"),
                    description=vp.get("description", "No description available"),
                    price=float(vp.get("price", 0.0)),
                    photoUrl=vp.get("photoUrl", "https://via.placeholder.com/150")
                )
                combined_products.append(product)

        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch data from {vendor_url}: {e}")

    if not combined_products:
        raise HTTPException(status_code=404, detail="No products found from vendors")

    # send to kafka
    producer.send('product_data', [product.dict() for product in combined_products])
    return combined_products

dummy_products = [
    Product(id=1, name="Product A", description="Description of Product A", price=29.99, photoUrl="https://via.placeholder.com/150"),
    Product(id=2, name="Product B", description="Description of Product B", price=49.99, photoUrl="https://via.placeholder.com/150"),
    Product(id=3, name="Product C", description="Description of Product C", price=19.99, photoUrl="https://via.placeholder.com/150")
]

# get by id
@app.get("/products/{product_id}", response_model=Product)
async def get_product(product_id: int):
    try:
        # dummy search
        product = next((p for p in dummy_products if p.id == product_id), None)
        if product:
            return product
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")

    raise HTTPException(status_code=404, detail="Product not found")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
