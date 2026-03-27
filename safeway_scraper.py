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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ── Store locations (name, postal code) ───────────────────────────────────────
LOCATIONS = [
    ("Beddington",     "T3K2A8"),
    ("Dalhousie",      "T3A5R8"),
    ("Beacon Heights", "T2E2S6"),
    ("Castleridge",    "T3J3J8"),
    ("Market Mall",    "T3A0E2"),
    ("North Hill",     "T2N1M7"),
    ("Crowfoot",       "T3G2L5"),
    ("Kensington",     "T2N1V9"),
    ("Montgomery",     "T3B0N3"),
    ("Southcentre",    "T2J6S1"),
    ("Woodbine",       "T2W4N4"),
    ("Southtrail",     "T2Z3V8"),
    ("Shawnessy",      "T2Y2Z3"),
    ("Beltline",       "T2R0E6"),
]

# ── Categories to scrape ───────────────────────────────────────────────────────
CATEGORIES = [
    ("Bread & Bakery", "https://www.safeway.ca/products/category/Bread__&__Bakery"),
    ("Cheese & Deli",  "https://www.safeway.ca/products/category/Cheese__&__Deli"),
    ("Meat",           "https://www.safeway.ca/products/category/Meat"),
    ("Seafood",        "https://www.safeway.ca/products/category/Seafood"),
    ("Fruit and Veg",  "https://www.safeway.ca/products/category/Fresh__Fruits__&__Vegetables"),
    ("Plant Based",    "https://www.safeway.ca/products/category/Plant__Based"),
    ("Dairy & Eggs",   "https://www.safeway.ca/products/category/Dairy__&__Eggs"),
    ("Pantry",         "https://www.safeway.ca/products/category/Pantry"),
    ("Snacks & Candy", "https://www.safeway.ca/products/category/Snacks__&__Candy"),
    ("Drinks",         "https://www.safeway.ca/products/category/Drinks"),
    ("Frozen",         "https://www.safeway.ca/products/category/Frozen"),
    ("International",  "https://www.safeway.ca/products/category/International__Foods"),
]

BASE_URL   = "https://www.safeway.ca"
STORE_NAME = "Safeway"
MAX_PAGES  = 50


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
        # Add in_stock column to existing tables that were created before this fix
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
        raise


def insert_products(products):
    """Upsert a list of product dicts into the database.
    INSERT new rows, or UPDATE existing ones if the product_id already exists.
    This replaces the old append-only CSV import approach."""
    if not products:
        return

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


def select_store(driver, postal_code):
    print(f"  Selecting store for postal code: {postal_code}")
    driver.get(BASE_URL)
    time.sleep(5)
    dismiss_popups(driver)

    for sel in ['[aria-label*="store" i]', '[aria-label*="location" i]',
                'button[class*="store" i]', '[data-testid*="store" i]',
                '[class*="store-selector" i]', '[class*="location-selector" i]']:
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, sel):
                if el.is_displayed():
                    driver.execute_script("arguments[0].click();", el)
                    time.sleep(2)
                    break
            else:
                continue
            break
        except Exception:
            pass

    typed = False
    for sel in ['input[placeholder*="postal" i]', 'input[placeholder*="city" i]',
                'input[placeholder*="location" i]', 'input[type="search"]', 'input[type="text"]']:
        try:
            for inp in driver.find_elements(By.CSS_SELECTOR, sel):
                if inp.is_displayed():
                    inp.clear()
                    inp.send_keys(postal_code)
                    time.sleep(2)
                    typed = True
                    break
        except Exception:
            pass
        if typed:
            break

    if not typed:
        print(f"  WARNING: Could not find store input for {postal_code}")
        return False

    for sel in ['[class*="store-result" i]', '[class*="location-result" i]',
                '[data-testid*="store-result" i]', 'li[class*="store" i]', '[role="option"]']:
        try:
            result = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, sel))
            )
            driver.execute_script("arguments[0].click();", result)
            time.sleep(3)
            print("  Store selected.")
            return True
        except Exception:
            pass

    print(f"  WARNING: Could not select store result. Scraping anyway.")
    return False


def scroll_page(driver):
    for i in range(1, 6):
        driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {i/5});")
        time.sleep(1.5)
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(1)


def click_next(driver):
    dismiss_popups(driver)
    for el in driver.find_elements(By.CSS_SELECTOR, 'button, a, [role="button"]'):
        label    = (el.text or "").lower().strip()
        aria     = (el.get_attribute("aria-label") or "").lower()
        disabled = (el.get_attribute("disabled") or
                    el.get_attribute("aria-disabled") == "true" or
                    "disabled" in (el.get_attribute("class") or ""))
        if not disabled and (label in {"next", "load more", "show more", "›", "→"} or "next" in aria):
            try:
                driver.execute_script("arguments[0].click();", el)
                return True
            except Exception:
                pass
    return False


# ── Scraping logic ─────────────────────────────────────────────────────────────

def parse_products(html, category_name, store_name):
    soup = BeautifulSoup(html, "html.parser")
    # Safeway uses product links with aria-label for the product name
    links = [a for a in soup.select('a[href*="/products/"]')
             if "/category/" not in a.get("href", "")]
    products = []
    seen = set()

    for link in links:
        product_url = link.get("href", "")
        if not product_url.startswith("http"):
            product_url = BASE_URL + product_url

        product_id = uuid.uuid4().hex

        name = link.get("aria-label", "")
        name = re.sub(r"Click here to go to ", "", name, flags=re.IGNORECASE)
        name = re.sub(r" product detail page$", "", name, flags=re.IGNORECASE).strip()

        container = link.parent
        price = None
        if container:
            # Strip parenthesised per-unit price expressions like "($47.03 per 100g)"
            # before scanning for dollar amounts — these are NOT the shelf price.
            container_text = re.sub(
                r"\(\s*\$[\d.]+\s+per\s+\d+\s*(?:g|ml|kg|l|oz|lb)\s*\)",
                "", container.get_text(), flags=re.IGNORECASE
            )
            MIN_PLAUSIBLE_PRICE = 0.25
            MAX_PLAUSIBLE_PRICE = 150.0
            raw_prices = re.findall(r"\$\s*(\d+\.?\d*)", container_text)
            plausible = [float(p) for p in raw_prices if MIN_PLAUSIBLE_PRICE <= float(p) <= MAX_PLAUSIBLE_PRICE]
            if plausible:
                # Take the smallest remaining value — on sale cards the crossed-out
                # was-price is larger than the now-price so min() gives the real price.
                price = min(plausible)
            elif raw_prices:
                price = min(float(p) for p in raw_prices)

        # ── Stock status ───────────────────────────────────────────────────────
        container_html = str(container) if container else ""
        in_stock = not bool(re.search(
            r'out.of.stock|unavailable|not available|sold.out|'
            r'class="[^"]*out-of-stock|class="[^"]*unavailable',
            container_html, re.IGNORECASE
        ))
        if in_stock and container:
            try:
                add_btn = container.find(attrs={"aria-label": re.compile(r"add.*cart", re.I)})
                if add_btn and (add_btn.get("disabled") or add_btn.get("aria-disabled") == "true"):
                    in_stock = False
            except (AttributeError, TypeError):
                pass

        # Search for the image inside the link first, then broaden upward.
        # Sale cards often wrap the img in a sibling element that isn't
        # reachable via link.parent alone.
        img = link.find("img")
        if not img and container:
            img = container.find("img")
        if not img and container and container.parent:
            img = container.parent.find("img")
        image_url = ""
        if img:
            image_url = img.get("src") or img.get("data-src") or ""

        key = f"{name.lower()}|{product_url}"
        if key in seen:
            continue
        seen.add(key)

        if not name or not price or not product_url:
            continue

        parsed_size, parsed_unit, package_size = parse_size(name)
        cpu, qty, unit, unit_type = calc_unit_price(price, parsed_size, parsed_unit)
        unit_price_display = (
            f"{package_size}, ${cpu}/{qty}{unit}"
            if cpu and qty and unit and package_size else package_size
        )

        products.append({
            "category":                    category_name.lower(),
            "comparable_unit_price":       cpu,
            "image_url":                   image_url,
            "in_stock":                    in_stock,
            "location":                    store_name,
            "name":                        name,
            "package_size":                package_size,
            "parsed_package_size/size":    parsed_size or "",
            "parsed_package_size/unit":    parsed_unit,
            "parsed_unit_price":           "",
            "parsed_unit_price/quantity":  qty or "",
            "parsed_unit_price/unit":      unit,
            "parsed_unit_price/unit_type": unit_type,
            "parsed_unit_price/value":     cpu,
            "price":                       price or "",
            "product_id":                  product_id,
            "product_hash":                hashlib.sha256(f"{name}|{STORE_NAME}|{store_name}".encode()).hexdigest(),
            "product_url":                 product_url,
            "selling_type":                "by_unit",
            "store":                       STORE_NAME,
            "unit_price":                  unit_price_display,
        })
    return products


def scrape_category(driver, category_name, category_url, store_name):
    print(f"    Scraping: {category_name}")
    driver.get(category_url)
    time.sleep(5)
    all_products, seen_ids, page = [], set(), 1

    while page <= MAX_PAGES:
        print(f"      Page {page}...", end=" ")
        scroll_page(driver)
        page_products = parse_products(driver.page_source, category_name, store_name)
        new = [p for p in page_products if (p["name"].lower()) not in seen_ids]
        for p in new:
            seen_ids.add(p["name"].lower())
        all_products.extend(new)
        print(f"+{len(new)} ({len(all_products)} total)")
        if not new:
            break
        if not click_next(driver):
            break
        page += 1
        time.sleep(6)

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
        for loc_index, (store_name, postal_code) in enumerate(LOCATIONS, 1):
            print(f"\n{'='*50}")
            print(f"Location [{loc_index}/{len(LOCATIONS)}]: {store_name} ({postal_code})")
            print(f"{'='*50}")
            select_store(driver, postal_code)
            categories = list(CATEGORIES)
            print(f"  Using {len(categories)} manually defined categories")
            loc_total = 0
            for i, (cat_name, cat_url) in enumerate(categories, 1):
                print(f"\n  [{i}/{len(categories)}]", end=" ")
                products = scrape_category(driver, cat_name, cat_url, store_name)
                if products:
                    insert_products(products)
                    loc_total += len(products)
                else:
                    print("    No products found, skipping.")
            print(f"\n  Done {store_name}: {loc_total} products processed.")
    finally:
        driver.quit()
    print("\nAll locations complete.")


if __name__ == "__main__":
    main()

