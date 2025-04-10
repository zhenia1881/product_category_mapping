from dataclasses import dataclass
from typing import Optional

@dataclass
class Product:
    product_id: int
    product_name: str

@dataclass
class Category:
    category_id: int
    category_name: str

@dataclass
class ProductCategoryLink:
    product_id: int
    category_id: int