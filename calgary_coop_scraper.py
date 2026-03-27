import os
import re
import time
import uuid
import hashlib
import mysql.connector
from db_connection import get_db_connection
from mysql.connector import Error
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By



# ── Store locations ────────────────────────────────────────────────────────────
LOCATIONS = [
    "crowfoot",
    "auburnbay",
    "shawnessy",
    "sunridge",
    "northland",
    "willow_park",
    "sage-hill",
    "hamptons",
    "brentwood",
    "rocky-ridge",
    "north-hill",
    "forest-lawn",
    "dalhousie",
    "west-springs",
    "monterey",
    "midtown",
]

# ── Categories to scrape ───────────────────────────────────────────────────────
CATEGORIES = [
    ("Fruits & Vegetables", "fruits_vegetables"),
    ("Meat & Seafood",      "meat_seafood"),
    ("Dairy & Eggs",        "dairy_eggs"),
    ("Deli",                "deli"),
    ("Bakery",              "bakery"),
    ("Frozen",              "frozen_food"),
    ("Pantry",              "pantry_food"),
    ("Snacks & Sweets",     "snacks_sweets"),
    ("Drinks",              "drinks"),
]

BASE_URL   = "https://shoponline.calgarycoop.com"
STORE_NAME = "Calgary Co-op"


# ── Size parsing helpers ───────────────────────────────────────────────────────
SIZE_PATTERNS = [
    (r"(\d+)x(\d+(?:\.\d+)?)\s*(g|ml|oz)", "multi"),
    (r"(\d+(?:\.\d+)?)\s*(kg|g|lb|lbs|oz|l|ml|litre|liter|gram|grams|pound|pounds)\b", "single"),
    (r"(\d+)\s*(pack|count|ct|ea|each|piece|pieces)\b", "single"),
]
UNIT_ALIASES = {
    "litre": "l", "liter": "l", "gram": "g", "grams": "g",
    "pound": "lb", "pounds": "lb", "lbs": "lb",
    "each": "ea", "piece": "ea", "pieces": "ea", "count": "ea", "ct": "ea",
}


# ── Database helpers ───────────────────────────────────────────────────────────

def or_none(value):
    """Convert empty strings to None so MySQL stores NULL instead of erroring
    on a DECIMAL column that received an empty string."""
    if value == "" or value is None:
        return None
    return value


def strip_unit_prices(text):
    """Remove unit prices, earn/save text, and other non-shelf-price dollar amounts."""
    text = re.sub(
        r"\(\s*\$[\d.]+\s+per\s+[\d.]+\s*(?:g|mg|ml|kg|l|oz|lb|lbs|ea|each|unit|ct|count)\s*\)",
        "", text, flags=re.IGNORECASE)
    text = re.sub(
        r"\$[\d.]+\s+per\s+[\d.]+\s*(?:g|mg|ml|kg|l|oz|lb|lbs|ea|each|unit|ct|count)",
        "", text, flags=re.IGNORECASE)
    text = re.sub(
        r"\$[\d.]+\s*/\s*[\d.]*\s*(?:g|mg|ml|kg|l|oz|lb|lbs|ea|each|unit|ct|count)",
        "", text, flags=re.IGNORECASE)
    text = re.sub(
        r"(?:earn|save|bonus|reward|scene\+?|redeem|off)[^$]*\$[\d.]+",
        "", text, flags=re.IGNORECASE)
    text = re.sub(
        r"(?:approx\.?|approximately)\s+\$[\d.]+\s*/\s*(?:kg|lb|lbs)",
        "", text, flags=re.IGNORECASE)
    return text


def setup_database():
    """Connect to the database and create the products table if it doesn't
    exist yet. Called once at the start of main() before scraping begins."""
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                product_id            VARCHAR(100)    NOT NULL,
                product_hash          CHAR(64)        NOT NULL,
                category              VARCHAR(100),
                name                  VARCHAR(500)    NOT NULL,
                store                 VARCHAR(255)    NOT NULL,
                location              VARCHAR(100),
                price                 DECIMAL(10, 2),
                comparable_unit_price DECIMAL(10, 4),
                unit_price            VARCHAR(255),
                package_size          VARCHAR(50),
                pkg_size_value        DECIMAL(10, 3),
                pkg_size_unit         VARCHAR(20),
                unit_price_quantity   DECIMAL(10, 3),
                unit_price_unit       VARCHAR(20),
                unit_price_unit_type  VARCHAR(50),
                unit_price_value      DECIMAL(10, 4),
                selling_type          VARCHAR(50),
                product_url           TEXT,
                image_url             TEXT,
                in_stock              TINYINT(1)      NOT NULL DEFAULT 1,
                last_updated          DATETIME        NOT NULL,
                PRIMARY KEY (product_id),
                UNIQUE KEY uq_product_hash (product_hash)
            )
        """)
        # Add in_stock column to existing tables created before this fix
        try:
            cursor.execute("""
                ALTER TABLE products
                ADD COLUMN in_stock TINYINT(1) NOT NULL DEFAULT 1
            """)
        except Exception:
            pass  # Column already exists
        # Add product_hash column and unique constraint for upsert matching
        try:
            cursor.execute("""
                ALTER TABLE products
                ADD COLUMN product_hash CHAR(64) NOT NULL DEFAULT '',
                ADD UNIQUE KEY uq_product_hash (product_hash)
            """)
        except Exception:
            pass  # Column/key already exists
        connection.commit()
        cursor.close()
        connection.close()
        print("✅ Database table ready.")
    except Error as e:
        print(f"❌ Database setup failed: {e}")
        raise  # Stop the script — no point scraping if we can't save


def insert_products(products):
    """Upsert a list of product dicts into the database.
    INSERT new rows, or UPDATE existing ones if the product_id already exists.
    This replaces the old append-only CSV import approach."""
    if not products:
        return

    # The upsert query: try to INSERT; if product_id already exists (duplicate
    # PRIMARY KEY), run the UPDATE clause instead. No duplicates, ever.
    upsert_query = """
        INSERT INTO products (
            product_id, product_hash, category, name, store, location, price,
            comparable_unit_price, unit_price, package_size,
            pkg_size_value, pkg_size_unit, unit_price_quantity,
            unit_price_unit, unit_price_unit_type, unit_price_value,
            selling_type, product_url, image_url, in_stock, last_updated
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            category              = VALUES(category),
            name                  = VALUES(name),
            store                 = VALUES(store),
            location              = VALUES(location),
            price                 = VALUES(price),
            comparable_unit_price = VALUES(comparable_unit_price),
            unit_price            = VALUES(unit_price),
            package_size          = VALUES(package_size),
            pkg_size_value        = VALUES(pkg_size_value),
            pkg_size_unit         = VALUES(pkg_size_unit),
            unit_price_quantity   = VALUES(unit_price_quantity),
            unit_price_unit       = VALUES(unit_price_unit),
            unit_price_unit_type  = VALUES(unit_price_unit_type),
            unit_price_value      = VALUES(unit_price_value),
            selling_type          = VALUES(selling_type),
            product_url           = VALUES(product_url),
            image_url             = VALUES(image_url),
            in_stock              = VALUES(in_stock),
            last_updated          = VALUES(last_updated)
    """

    now = datetime.now()
    success_count = 0
    error_count = 0

    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        for product in products:
            try:
                # Map the scraper's slash-keyed dict fields to DB column order.
                # or_none() converts "" to None so DECIMAL columns receive NULL
                # rather than an empty string (which would cause a MySQL error).
                values = (
                    product.get("product_id"),
                    product.get("product_hash"),
                    product.get("category"),
                    product.get("name"),
                    product.get("store"),
                    product.get("location"),
                    or_none(product.get("price")),
                    or_none(product.get("comparable_unit_price")),
                    product.get("unit_price"),
                    product.get("package_size"),
                    or_none(product.get("parsed_package_size/size")),
                    product.get("parsed_package_size/unit"),
                    or_none(product.get("parsed_unit_price/quantity")),
                    product.get("parsed_unit_price/unit"),
                    product.get("parsed_unit_price/unit_type"),
                    or_none(product.get("parsed_unit_price/value")),
                    product.get("selling_type"),
                    product.get("product_url"),
                    product.get("image_url"),
                    1 if product.get("in_stock", True) else 0,
                    now,
                )
                cursor.execute(upsert_query, values)
                success_count += 1
            except Error as e:
                print(f"  ⚠️  Row error for '{product.get('name', '?')}': {e}")
                error_count += 1

        connection.commit()
        cursor.close()
        connection.close()
        print(f"  ✅ Saved {success_count} products ({error_count} errors).")

    except Error as e:
        print(f"  ❌ Database connection error: {e}")


# ── Size / unit price calculation ─────────────────────────────────────────────

def parse_size(name):
    for pattern, kind in SIZE_PATTERNS:
        m = re.search(pattern, name, re.IGNORECASE)
        if not m:
            continue
        if kind == "multi":
            size = float(m.group(1)) * float(m.group(2))
            unit = m.group(3).lower()
        else:
            size = float(m.group(1))
            unit = m.group(2).lower()
        unit = UNIT_ALIASES.get(unit, unit)
        return size, unit, f"{size}{unit}"
    return None, "", ""


def calc_unit_price(price, parsed_size, parsed_unit):
    if not (price and parsed_size and parsed_unit):
        return "", "", "", ""
    if parsed_unit in ("g", "kg", "ml", "l", "lb", "oz"):
        unit_type = "weight"
        if parsed_unit == "kg":   cpu, qty, unit = price / (parsed_size * 10), 100, "g"
        elif parsed_unit == "g":  cpu, qty, unit = price / (parsed_size / 100), 100, "g"
        elif parsed_unit == "l":  cpu, qty, unit = price / (parsed_size * 10), 100, "ml"
        elif parsed_unit == "ml": cpu, qty, unit = price / (parsed_size / 100), 100, "ml"
        elif parsed_unit == "lb": cpu, qty, unit = (price / parsed_size * 2.20462) / 10, 100, "g"
        elif parsed_unit == "oz": cpu, qty, unit = price / (parsed_size * 28.3495 / 100), 100, "g"
        else:                     cpu, qty, unit = price, 1, parsed_unit
    elif parsed_unit in ("ea", "pack"):
        unit_type, cpu, qty, unit = "count", price, 1, "ea"
    else:
        return "", "", "", ""
    return f"{cpu:.2f}", qty, unit, unit_type


# ── Browser helpers ────────────────────────────────────────────────────────────

def dismiss_popups(driver):
    try:
        driver.execute_script("""
            ['#onetrust-consent-sdk','#onetrust-banner-sdk','.onetrust-pc-dark-filter']
            .forEach(sel => { var el = document.querySelector(sel); if (el) el.remove(); });
        """)
    except Exception:
        pass


# ── Scraping logic ─────────────────────────────────────────────────────────────

def parse_products(html, category_name, location):
    soup = BeautifulSoup(html, "html.parser")
    containers = soup.select(".product-container")
    products = []
    seen = set()

    for container in containers:
        title_el = container.select_one(".pc-title-container h2")
        name = title_el.get_text(strip=True) if title_el else ""
        if not name or len(name) < 2:
            continue

        price = None
        MIN_PLAUSIBLE_PRICE = 0.50
        MAX_PLAUSIBLE_PRICE = 200.0
        for el in container.select('[class*="price"]'):
            el_text = el.get_text(strip=True)
            # Skip elements that look like unit prices (contain /kg, /lb, /100g, /ea, per)
            if re.search(r'/\s*(?:\d*\s*)?(?:kg|g|lb|lbs|ml|l|oz|ea|each|unit|ct)\b|per\s+\d', el_text, re.I):
                continue
            m = re.match(r"^\$\s*(\d+\.\d{2})", el_text)
            if m:
                candidate = float(m.group(1))
                if MIN_PLAUSIBLE_PRICE <= candidate <= MAX_PLAUSIBLE_PRICE:
                    price = candidate
                    break

        key = f"{name.lower()}|{price or 'NO_PRICE'}"
        if key in seen:
            continue
        seen.add(key)

        text = container.get_text()

        # ── Stock status ───────────────────────────────────────────────────────
        container_html = str(container)
        in_stock = not bool(re.search(
            r'out.of.stock|unavailable|not available|sold.out|'
            r'class="[^"]*out-of-stock|class="[^"]*unavailable',
            container_html, re.IGNORECASE
        ))
        if in_stock and hasattr(container, 'find'):
            try:
                add_btn = container.find(attrs={"aria-label": re.compile(r"add.*cart", re.I)})
                if add_btn and (add_btn.get("disabled") or add_btn.get("aria-disabled") == "true"):
                    in_stock = False
            except (AttributeError, TypeError):
                pass
        unit_price_text, per_kg_price = "", None
        for pattern, label in [
            (r"\$\s*(\d+\.?\d*)\s*/\s*kg", "/kg"),
            (r"\$\s*(\d+\.?\d*)\s*/\s*lb", "/lb"),
            (r"\$\s*(\d+\.?\d*)\s*/\s*100\s*g", "/100g"),
        ]:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                val = float(m.group(1))
                if label == "/kg":   per_kg_price, unit_price_text = val, f"${val}/kg"
                elif label == "/lb": per_kg_price, unit_price_text = val * 2.20462, f"${val}/lb"
                else:                per_kg_price, unit_price_text = val * 10, f"${val}/100g"
                break

        img = container.select_one(".pc-image-container img") or container.select_one("img")
        image_url = img["src"] if img else ""

        link = container.select_one("a")
        product_url = link["href"] if link else ""
        if product_url and not product_url.startswith("http"):
            product_url = BASE_URL + product_url

        product_id = uuid.uuid4().hex

        if not name or not price or not product_url:
            continue

        parsed_size, parsed_unit, package_size = parse_size(name)
        avg_m = re.search(r"average\s+\w+\s+is\s+(\d+)\s*g", text, re.IGNORECASE)
        if avg_m and not package_size:
            parsed_size, parsed_unit, package_size = int(avg_m.group(1)), "g", f"{avg_m.group(1)}g"

        if per_kg_price:
            cpu, qty, unit, unit_type = f"{per_kg_price / 10:.2f}", 100, "g", "weight"
        else:
            cpu, qty, unit, unit_type = calc_unit_price(price, parsed_size, parsed_unit)

        if cpu and qty and unit:
            unit_price_display = f"{package_size}, ${cpu}/{qty}{unit}" if package_size else f"${cpu}/{qty}{unit}"
        else:
            unit_price_display = package_size or unit_price_text

        selling_type = "by_weight" if per_kg_price and not parsed_unit else "by_unit"

        products.append({
            "category":                    category_name.lower(),
            "comparable_unit_price":       cpu,
            "image_url":                   image_url,
            "in_stock":                    in_stock,
            "location":                    location,
            "name":                        name,
            "package_size":                package_size,
            "parsed_package_size/size":    parsed_size or "",
            "parsed_package_size/unit":    parsed_unit,
            "parsed_unit_price":           unit_price_text,
            "parsed_unit_price/quantity":  qty or "",
            "parsed_unit_price/unit":      unit,
            "parsed_unit_price/unit_type": unit_type,
            "parsed_unit_price/value":     cpu,
            "price":                       price or "",
            "product_id":                  product_id,
            "product_hash":                hashlib.sha256(f"{name}|{STORE_NAME}|{location}".encode()).hexdigest(),
            "product_url":                 product_url,
            "selling_type":                selling_type,
            "store":                       STORE_NAME,
            "unit_price":                  unit_price_display,
        })
    return products


def scrape_category(driver, category_name, category_url, location):
    print(f"    Scraping: {category_name}")
    driver.get(category_url)
    time.sleep(5)
    dismiss_popups(driver)

    all_products, seen_ids = [], set()
    stall_rounds = 0
    MAX_STALLS = 3

    while True:
        current_height = driver.execute_script("return document.body.scrollHeight")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)

        try:
            cards = driver.find_elements(By.CSS_SELECTOR, ".product-container")
            if cards:
                driver.execute_script(
                    "arguments[0].scrollIntoView({behavior: 'smooth', block: 'end'});",
                    cards[-1]
                )
        except Exception:
            pass
        time.sleep(3)

        page_products = parse_products(driver.page_source, category_name, location)
        new = [p for p in page_products if p["name"].lower() not in seen_ids]
        for p in new:
            seen_ids.add(p["name"].lower())
        all_products.extend(new)

        new_height = driver.execute_script("return document.body.scrollHeight")
        print(f"      Scroll... +{len(new)} new ({len(all_products)} total)", end=" ")

        if len(new) == 0 and new_height == current_height:
            stall_rounds += 1
            print(f"[stall {stall_rounds}/{MAX_STALLS}]")
            if stall_rounds >= MAX_STALLS:
                print(f"    Reached bottom — {len(all_products)} total products")
                break
        else:
            stall_rounds = 0
            print()

    return all_products


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    # Create the table once before any scraping starts.
    # If the table already exists this does nothing — it never overwrites data.
    setup_database()

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(options=options)

    try:
        for loc_index, location in enumerate(LOCATIONS, 1):
            print(f"\n{'='*50}")
            print(f"Location [{loc_index}/{len(LOCATIONS)}]: {location}")
            print(f"{'='*50}")
            categories = [
                (name, f"{BASE_URL}/{location}/#/category/{slug}")
                for name, slug in CATEGORIES
            ]
            print(f"  Using {len(categories)} manually defined categories")
            loc_total = 0
            for i, (cat_name, cat_url) in enumerate(categories, 1):
                print(f"\n  [{i}/{len(categories)}]", end=" ")
                products = scrape_category(driver, cat_name, cat_url, location)
                if products:
                    insert_products(products)
                    loc_total += len(products)
                else:
                    print("    No products found, skipping.")
            print(f"\n  Done {location}: {loc_total} products processed.")
    finally:
        driver.quit()
    print("\nAll locations complete.")


if __name__ == "__main__":
    main()
