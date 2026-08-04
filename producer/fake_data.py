"""
fake_data.py
Generates realistic fake e-commerce events using Faker.

Event Types:
  - Order   : user places an order for a product
  - Payment : payment event tied to an order
  - Click   : user browsing/interaction event
  - Review  : user reviews a product
"""

import uuid
import random
from datetime import datetime, timezone
from faker import Faker

fake = Faker("en_IN")   # Indian locale for realistic ₹ data
Faker.seed(0)

# ─────────────────────────────────────────────
# Static Reference Data
# ─────────────────────────────────────────────

PRODUCTS = [
    {"id": "P001", "name": "Nike Air Max",           "category": "Footwear",     "base_price": 8999},
    {"id": "P002", "name": "Apple iPhone 15",         "category": "Electronics",  "base_price": 79999},
    {"id": "P003", "name": "Samsung Galaxy S24",      "category": "Electronics",  "base_price": 69999},
    {"id": "P004", "name": "Levi's 511 Jeans",        "category": "Clothing",     "base_price": 3499},
    {"id": "P005", "name": "Sony WH-1000XM5",         "category": "Electronics",  "base_price": 24999},
    {"id": "P006", "name": "Adidas Superstar",         "category": "Footwear",     "base_price": 7499},
    {"id": "P007", "name": "HP Pavilion Laptop",       "category": "Electronics",  "base_price": 54999},
    {"id": "P008", "name": "Instant Pot Duo 7-in-1",  "category": "Kitchen",      "base_price": 6999},
    {"id": "P009", "name": "Yoga Mat Pro",             "category": "Sports",       "base_price": 1299},
    {"id": "P010", "name": "Puma Running Shorts",      "category": "Clothing",     "base_price": 1099},
    {"id": "P011", "name": "Kindle Paperwhite",        "category": "Electronics",  "base_price": 14999},
    {"id": "P012", "name": "Boat Airdopes 141",        "category": "Electronics",  "base_price": 1299},
    {"id": "P013", "name": "Prestige Cooker 5L",       "category": "Kitchen",      "base_price": 2199},
    {"id": "P014", "name": "Ray-Ban Aviator",          "category": "Accessories",  "base_price": 5999},
    {"id": "P015", "name": "Whey Protein Gold",        "category": "Sports",       "base_price": 3499},
    {"id": "P016", "name": "JBL Flip 6",               "category": "Electronics",  "base_price": 9999},
    {"id": "P017", "name": "Cotton Formal Shirt",      "category": "Clothing",     "base_price": 899},
    {"id": "P018", "name": "Bamboo Cutting Board",     "category": "Kitchen",      "base_price": 599},
    {"id": "P019", "name": "Resistance Band Set",      "category": "Sports",       "base_price": 799},
    {"id": "P020", "name": "Fossil Gen 6 Watch",       "category": "Accessories",  "base_price": 19999},
]

COUNTRIES = [
    "India", "India", "India", "India",   # Heavy India bias
    "USA", "UK", "Canada", "Australia",
    "Germany", "Singapore", "UAE", "Japan",
]

DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.60, 0.30, 0.10]

BROWSERS = ["Chrome", "Safari", "Firefox", "Edge", "Samsung Internet"]

PAYMENT_TYPES = ["UPI", "Credit Card", "Debit Card", "Net Banking", "COD"]
PAYMENT_WEIGHTS = [0.40, 0.25, 0.15, 0.10, 0.10]

PAYMENT_GATEWAYS = {
    "UPI":          ["PhonePe", "Google Pay", "Paytm", "BHIM"],
    "Credit Card":  ["Razorpay", "PayU", "CCAvenue"],
    "Debit Card":   ["Razorpay", "PayU"],
    "Net Banking":  ["HDFC", "SBI", "ICICI", "Axis"],
    "COD":          [None],
}

PAGES = ["home", "search", "product", "cart", "checkout", "wishlist"]
ACTIONS = ["view", "click", "add_to_cart", "remove_from_cart", "wishlist", "scroll"]

SENTIMENTS = {
    5: "positive",
    4: "positive",
    3: "neutral",
    2: "negative",
    1: "negative",
}

ORDER_STATUSES = ["confirmed", "processing", "shipped", "delivered", "cancelled"]
PAYMENT_STATUSES_WEIGHTS = [("success", 0.85), ("failed", 0.10), ("pending", 0.05)]


def _now_iso() -> str:
    """Return current UTC time as ISO8601 string."""
    return datetime.now(timezone.utc).isoformat()


def _random_product() -> dict:
    """Pick a random product from the catalogue."""
    return random.choice(PRODUCTS)


def _random_price_variance(base_price: int) -> float:
    """Add ±10% variance to the base price."""
    variance = random.uniform(0.90, 1.10)
    return round(base_price * variance, 2)


def _weighted_choice(choices_weights: list[tuple]) -> str:
    """Choose from a list of (value, weight) tuples."""
    choices, weights = zip(*choices_weights)
    return random.choices(choices, weights=weights, k=1)[0]


# ─────────────────────────────────────────────
# Event Generators
# ─────────────────────────────────────────────

def generate_order_event() -> dict:
    """
    Generate a realistic order event.

    Returns:
        dict: Order event with all required fields.
    """
    product  = _random_product()
    price    = _random_price_variance(product["base_price"])
    quantity = random.choices([1, 2, 3], weights=[0.75, 0.20, 0.05])[0]

    return {
        "event_type":    "order",
        "order_id":      str(uuid.uuid4()),
        "user_id":       str(uuid.uuid4()),
        "product_id":    product["id"],
        "product_name":  product["name"],
        "category":      product["category"],
        "price":         price,
        "quantity":      quantity,
        "total_amount":  round(price * quantity, 2),
        "country":       random.choice(COUNTRIES),
        "device":        random.choices(DEVICES, weights=DEVICE_WEIGHTS)[0],
        "browser":       random.choice(BROWSERS),
        "status":        random.choices(ORDER_STATUSES, weights=[0.5, 0.2, 0.15, 0.1, 0.05])[0],
        "timestamp":     _now_iso(),
    }


def generate_payment_event(order_id: str | None = None, user_id: str | None = None,
                            amount: float | None = None) -> dict:
    """
    Generate a realistic payment event.
    Optionally linked to an existing order.

    Returns:
        dict: Payment event.
    """
    payment_type = random.choices(PAYMENT_TYPES, weights=PAYMENT_WEIGHTS)[0]
    gateway      = random.choice(PAYMENT_GATEWAYS[payment_type])
    status       = _weighted_choice(PAYMENT_STATUSES_WEIGHTS)

    if amount is None:
        product = _random_product()
        amount  = _random_price_variance(product["base_price"])

    return {
        "event_type":    "payment",
        "payment_id":    str(uuid.uuid4()),
        "order_id":      order_id or str(uuid.uuid4()),
        "user_id":       user_id  or str(uuid.uuid4()),
        "payment_type":  payment_type,
        "amount":        round(amount, 2),
        "status":        status,
        "gateway":       gateway,
        "timestamp":     _now_iso(),
    }


def generate_click_event() -> dict:
    """
    Generate a realistic browsing/click event.

    Returns:
        dict: Click event.
    """
    product = _random_product()

    return {
        "event_type":    "click",
        "click_id":      str(uuid.uuid4()),
        "user_id":       str(uuid.uuid4()),
        "product_id":    product["id"],
        "session_id":    str(uuid.uuid4()),
        "page":          random.choice(PAGES),
        "action":        random.choice(ACTIONS),
        "duration_sec":  random.randint(1, 300),
        "device":        random.choices(DEVICES, weights=DEVICE_WEIGHTS)[0],
        "timestamp":     _now_iso(),
    }


def generate_review_event() -> dict:
    """
    Generate a realistic product review event.

    Returns:
        dict: Review event.
    """
    product  = _random_product()
    rating   = random.choices([1, 2, 3, 4, 5], weights=[0.05, 0.10, 0.15, 0.35, 0.35])[0]

    return {
        "event_type":    "review",
        "review_id":     str(uuid.uuid4()),
        "user_id":       str(uuid.uuid4()),
        "product_id":    product["id"],
        "product_name":  product["name"],
        "rating":        rating,
        "sentiment":     SENTIMENTS[rating],
        "verified":      random.choice([True, False]),
        "timestamp":     _now_iso(),
    }


# ─────────────────────────────────────────────
# Quick test
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import json
    print("=== Order Event ===")
    print(json.dumps(generate_order_event(), indent=2))
    print("\n=== Payment Event ===")
    print(json.dumps(generate_payment_event(), indent=2))
    print("\n=== Click Event ===")
    print(json.dumps(generate_click_event(), indent=2))
    print("\n=== Review Event ===")
    print(json.dumps(generate_review_event(), indent=2))
