from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import requests
from kafka import KafkaProducer
import json
import sqlite3
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI()

class Product(BaseModel):
    id: int
    name: str
    description: str
    price: float
    photoUrl: str

# test
products = [
    Product(id=1, name="Product A", description="Description of Product A", price=29.99, photoUrl="https://via.placeholder.com/150"),
    Product(id=2, name="Product B", description="Description of Product B", price=49.99, photoUrl="https://via.placeholder.com/150"),
    Product(id=3, name="Product C", description="Description of Product C", price=19.99, photoUrl="https://via.placeholder.com/150")
]

VENDOR_APIS = [
    "http://localhost:8001/vendor1/products",
    "http://localhost:8002/vendor2/products",
    "http://localhost:8003/vendor3/products"
]

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# off-line DB
sqlite_conn = sqlite3.connect("local_products.db")
sqlite_cursor = sqlite_conn.cursor()
sqlite_cursor.execute('''CREATE TABLE IF NOT EXISTS products (
                            id INTEGER PRIMARY KEY,
                            name TEXT,
                            description TEXT,
                            price REAL,
                            photoUrl TEXT
                         )''')
sqlite_conn.commit()

# DB
try:
    pg_conn = psycopg2.connect(
        dbname="PRODUCTS",
        user="sadullahcihan",
        password="123",
        host="localhost",
        port="5432",
        cursor_factory=RealDictCursor
    )
    pg_cursor = pg_conn.cursor()
except Exception as e:
    print(f"Error connecting to DB: {e}")

# home
@app.get("/")
async def read_root():
    return {"message": "Welcome!"}

# get all
@app.get("/products", response_model=List[Product])
async def get_products():
    combined_products = products.copy()  # Include in-memory products for demonstration

    for vendor_url in VENDOR_APIS:
        try:
            response = requests.get(vendor_url)
            response.raise_for_status()
            vendor_products = response.json()

            for vp in vendor_products:
                product = Product(
                    id=vp.get("id", 0),
                    name=vp.get("name", "Unnamed Product"),
                    description=vp.get("description", "No description available"),
                    price=float(vp.get("price", 0.0)),
                    photoUrl=vp.get("photoUrl", "https://via.placeholder.com/150")
                )
                combined_products.append(product)

                # storing in off-line DB
                sqlite_cursor.execute('''INSERT OR REPLACE INTO products (id, name, description, price, photoUrl)
                                         VALUES (?, ?, ?, ?, ?)''',
                                      (product.id, product.name, product.description, product.price, product.photoUrl))
                sqlite_conn.commit()

        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch data from {vendor_url}: {e}")

    if not combined_products:
        raise HTTPException(status_code=404, detail="No products found from vendors")

    producer.send('product_data', [product.dict() for product in combined_products])

    # add to DB
    try:
        for product in combined_products:
            pg_cursor.execute('''INSERT INTO products (id, name, description, price, photoUrl)
                                 VALUES (%s, %s, %s, %s, %s)
                                 ON CONFLICT (id) DO UPDATE
                                 SET name = EXCLUDED.name, description = EXCLUDED.description,
                                     price = EXCLUDED.price, photoUrl = EXCLUDED.photoUrl''',
                              (product.id, product.name, product.description, product.price, product.photoUrl))
        pg_conn.commit()
    except Exception as e:
        print(f"Error inserting into PostgreSQL: {e}")

    return combined_products

# get by id
@app.get("/products/{product_id}", response_model=Product)
async def get_product(product_id: int):
    for product in products:
        if product.id == product_id:
            return product

    sqlite_cursor.execute("SELECT * FROM products WHERE id = ?", (product_id,))
    product_row = sqlite_cursor.fetchone()

    if product_row:
        product = Product(
            id=product_row[0],
            name=product_row[1],
            description=product_row[2],
            price=product_row[3],
            photoUrl=product_row[4]
        )
        return product

    raise HTTPException(status_code=404, detail="Product not found")

# save
@app.post("/products", response_model=Product)
async def add_product(product: Product):
    products.append(product)
    sqlite_cursor.execute('''INSERT OR REPLACE INTO products (id, name, description, price, photoUrl)
                             VALUES (?, ?, ?, ?, ?)''',
                          (product.id, product.name, product.description, product.price, product.photoUrl))
    sqlite_conn.commit()

    try:
        pg_cursor.execute('''INSERT INTO products (id, name, description, price, photoUrl)
                             VALUES (%s, %s, %s, %s, %s)''',
                          (product.id, product.name, product.description, product.price, product.photoUrl))
        pg_conn.commit()
    except Exception as e:
        print(f"Error inserting into PostgreSQL: {e}")

    return product

# update
@app.put("/products/{product_id}", response_model=Product)
async def update_product(product_id: int, updated_product: Product):
    for index, product in enumerate(products):
        if product.id == product_id:
            products[index] = updated_product

            sqlite_cursor.execute('''UPDATE products SET name = ?, description = ?, price = ?, photoUrl = ?
                                     WHERE id = ?''',
                                  (updated_product.name, updated_product.description, updated_product.price,
                                   updated_product.photoUrl, updated_product.id))
            sqlite_conn.commit()

            try:
                pg_cursor.execute('''UPDATE products SET name = %s, description = %s, price = %s, photoUrl = %s
                                     WHERE id = %s''',
                                  (updated_product.name, updated_product.description, updated_product.price,
                                   updated_product.photoUrl, updated_product.id))
                pg_conn.commit()
            except Exception as e:
                print(f"Error updating PostgreSQL: {e}")

            return updated_product

    raise HTTPException(status_code=404, detail="Product not found")

# delete
@app.delete("/products/{product_id}", response_model=Product)
async def delete_product(product_id: int):
    for index, product in enumerate(products):
        if product.id == product_id:
            deleted_product = products.pop(index)

            sqlite_cursor.execute("DELETE FROM products WHERE id = ?", (product_id,))
            sqlite_conn.commit()

            try:
                pg_cursor.execute("DELETE FROM products WHERE id = %s", (product_id,))
                pg_conn.commit()
            except Exception as e:
                print(f"Error deleting from PostgreSQL: {e}")

            return deleted_product

    raise HTTPException(status_code=404, detail="Product not found")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
