from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional

app = FastAPI()

class Product(BaseModel):
    id: int
    name: str
    description: str
    price: float
    photoUrl: str

# product DB
products = [
    Product(id=1, name="Product A", description="Description of Product A", price=29.99, photoUrl="https://via.placeholder.com/150"),
    Product(id=2, name="Product B", description="Description of Product B", price=49.99, photoUrl="https://via.placeholder.com/150"),
    Product(id=3, name="Product C", description="Description of Product C", price=19.99, photoUrl="https://via.placeholder.com/150")
]

# homepage
@app.get("/")
async def read_root():
    return {"message": "Welcome to the Product Catalog API!"}

# get list
@app.get("/products", response_model=List[Product])
async def get_products():
    return products

# get by id
@app.get("/products/{product_id}", response_model=Product)
async def get_product(product_id: int):
    for product in products:
        if product.id == product_id:
            return product
    return {"error": "Product not found"}

# save
@app.post("/products", response_model=Product)
async def add_product(product: Product):
    products.append(product)
    return product

# update
@app.put("/products/{product_id}", response_model=Product)
async def update_product(product_id: int, updated_product: Product):
    for index, product in enumerate(products):
        if product.id == product_id:
            products[index] = updated_product
            return updated_product
    return {"error": "Product not found"}

# delete
@app.delete("/products/{product_id}", response_model=Product)
async def delete_product(product_id: int):
    for index, product in enumerate(products):
        if product.id == product_id:
            deleted_product = products.pop(index)
            return deleted_product
    return {"error": "Product not found"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
