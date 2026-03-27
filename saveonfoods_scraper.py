import os
import re
import time
import uuid
import hashlib
import subprocess
import mysql.connector
from db_connection import get_db_connection
from mysql.connector import Error
from datetime import datetime
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ── Store locations (name, rsid) ──────────────────────────────────────────────
# Save-On-Foods uses a store rsid in the URL rather than postal codes
LOCATIONS = [
    ("Panorama Hills",      "6604"),
    ("Hunterhorn",          "6629"),
    ("University District", "6647"),
    ("Trinity Hill",        "6631"),
    ("Mount Royal",         "6638"),
    ("Trans Canada",        "6636"),
    ("Richmond Square",     "6633"),
    ("Heritage",            "6634"),
    ("Walden",              "6606"),
    ("Seton",               "6632"),
]

# ── Categories to scrape ───────────────────────────────────────────────────────
CATEGORIES = [
    ("Fresh Fruit",                "fruits-vegetables/fresh-fruit-id-30682"),
    ("Fresh Juice & Smoothies",    "fruits-vegetables/fresh-juice-smoothies-id-30723"),
    ("Fresh Vegetables",           "fruits-vegetables/fresh-vegetables-id-30694"),
    ("Salad Kits & Greens",        "fruits-vegetables/salad-kits-greens-essentials-id-30717"),
    ("Dried Snack Fruit & Nuts",   "fruits-vegetables/dried-snack-fruit-nuts-id-30725"),
    ("Dressing & Dips",            "fruits-vegetables/dressing-dips-id-30722"),
    ("Bagels & English Muffins",   "bakery/bagels-english-muffins-id-30847"),
    ("Breads",                     "bakery/breads-id-30850"),
    ("Cakes",                      "bakery/cakes-id-30888"),
    ("Dessert & Pastries",         "bakery/dessert-pastries-id-30879"),
    ("Frozen Cakes & Pies",        "frozen-bakery/frozen-cakes-pies-id-30903"),
    ("Pies & Tarts",               "bakery/pies-tarts-id-30894"),
    ("Pitas, Flatbread & Wraps",   "pitas-flatbread-wraps-id-30899"),
    ("Butter & Margarine",         "dairy-eggs/butter-margarine-id-30907"),
    ("Cheese",                     "dairy-eggs/cheese-id-30910"),
    ("Chilled Juice & Drinks",     "dairy-eggs/chilled-juice-drinks-id-30920"),
    ("Dough Products",             "dairy-eggs/dough-products-id-30929"),
    ("Eggs & Substitutes",         "dairy-eggs/eggs-substitutes-id-30919"),
    ("Milk & Creams",              "dairy-eggs/milk-creams-id-30930"),
    ("Milk Substitutes",           "dairy-eggs/milk-substitutes-id-30939"),
    ("Pudding & Desserts",         "dairy-eggs/pudding-desserts-id-30943"),
    ("Sour Cream & Dips",          "dairy-eggs/sour-cream-dips-id-30944"),
    ("Yogurt",                     "dairy-eggs/yogurt-id-30945"),
    ("Frozen Appetizers & Snacks", "frozen/frozen-appetizers-snacks-id-30950"),
    ("Frozen Bakery",              "frozen/frozen-bakery-id-30956"),
    ("Ice Cubes & Blocks",         "frozen-beverages-ice/ice-cubes-blocks-id-30966"),
    ("Frozen Breakfast",           "frozen/frozen-breakfast-id-30967"),
    ("Frozen Fruit",               "frozen/frozen-fruit-id-30971"),
    ("Frozen Meals & Sides",       "frozen/frozen-meals-sides-id-30976"),
    ("Frozen Meat",                "frozen/frozen-meat-id-30982"),
    ("Frozen Pizza",               "frozen/frozen-pizza-id-30993"),
    ("Frozen Seafood",             "frozen/frozen-seafood-id-30999"),
    ("Frozen Vegetables",          "frozen/frozen-vegetables-id-31002"),
    ("Ice Cream & Desserts",       "frozen/ice-cream-desserts-id-31008"),
    ("Asian",                      "international-foods/asian-id-31406"),
    ("European",                   "international-foods/european-id-31439"),
    ("Indian & Middle Eastern",    "international-foods/indian-middle-eastern-id-31415"),
    ("Latin & Mexican",            "international-foods/latin-mexican-id-31432"),
    ("Mediterranean",              "international-foods/mediterranean-id-31445"),
    ("Bacon",                      "meat-seafood/bacon-id-30817"),
    ("Beef & Veal",                "meat-seafood/beef-veal-id-30792"),
    ("Chicken & Turkey",           "meat-seafood/chicken-turkey-id-30798"),
    ("Fish",                       "meat-seafood/fish-id-30827"),
    ("Frozen Meat",                "meat-seafood/frozen-meat-id-30830"),
    ("Frozen Seafood",             "meat-seafood/frozen-seafood-id-30842"),
    ("Hot Dogs & Sausages",        "meat-seafood/hot-dogs-sausages-id-30818"),
    ("Lamb",                       "meat-seafood/lamb-id-30815"),
    ("Meat Alternatives",          "meat-seafood/meat-alternatives-id-30821"),
    ("Pork & Ham",                 "meat-seafood/pork-ham-id-30807"),
    ("Shrimp & Shellfish",         "meat-seafood/shrimp-shell-fish-id-30828"),
    ("Smoked & Cured Fish",        "meat-seafood/smoked-cured-fish-id-30829"),
    ("Baking Goods",               "pantry/baking-goods-id-30373"),
    ("Breakfast",                  "pantry/breakfast-id-30481"),
    ("Canned & Packaged",          "pantry/canned-packaged-id-30527"),
    ("Condiments & Toppings",      "pantry/condiments-toppings-id-30596"),
    ("Herbs, Spices & Seasonings", "pantry/herbs-spices-seasonings-id-30635"),
    ("Marinades & Sauces",         "pantry/marinates-sauces-id-30614"),
    ("Oils & Vinegars",            "pantry/oils-vinegars-id-30625"),
    ("Pasta, Sauces & Grains",     "pantry/pasta-sauces-grains-id-30652"),
    ("Beverages",                  "beverages-id-30385"),
    ("Bulk",                       "bulk-id-31287"),
    ("Candy",                      "candy-id-30504"),
    ("Snacks",                     "snacks-id-30511"),
]

BASE_URL   = "https://www.saveonfoods.com"
MAX_PAGES  = 50
STORE_NAME = "Save-On-Foods"


# ── Size parsing helpers ───────────────────────────────────────────────────────
# Note: Save-On-Foods uses a different SIZE_PATTERNS list from the other scrapers
# because their product names follow a slightly different format
SIZE_PATTERNS = [
    (r"(\d+(?:\.\d+)?)\s*(g|kg|ml|l|lb|lbs|oz|pound|pounds|gram|grams|litre|liter|millilitre|milliliter)\b", "single"),
    (r",\s*(\d+)\s*(each|ea|count|piece)\b", "single"),
]
UNIT_ALIASES = {
    "litre": "l", "liter": "l", "millilitre": "ml", "milliliter": "ml",
    "gram": "g", "grams": "g", "pound": "lb", "pounds": "lb", "lbs": "lb",
    "each": "ea", "piece": "ea", "count": "ea",
}


# ── Database helpers ───────────────────────────────────────────────────────────

def or_none(value):
    """Convert empty strings to None so MySQL stores NULL instead of erroring
    on a DECIMAL column that received an empty string."""
    if value == "" or value is None:
        return None
    return value


def safe_str(tag):
    """Convert a BS4 tag to string safely across BS4 versions."""
    if tag is None:
        return ""
    try:
        return str(tag)
    except (TypeError, AttributeError):
        try:
            return tag.decode()
        except Exception:
            return tag.get_text() if hasattr(tag, 'get_text') else ""


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
    for pattern, _ in SIZE_PATTERNS:
        m = re.search(pattern, name, re.IGNORECASE)
        if not m:
            continue
        size = float(m.group(1))
        unit = UNIT_ALIASES.get(m.group(2).lower(), m.group(2).lower())
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
        else:                     cpu, qty, unit = price, 1, parsed_unit
    elif parsed_unit in ("ea", "each", "count", "piece"):
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


def click_next(driver):
    """Click the Next Page button using Save-On-Foods' stable data-testid attribute."""
    dismiss_popups(driver)
    try:
        btn = driver.find_element(By.CSS_SELECTOR, '[data-testid="nextPage-button-testId"]')
        if btn.get_attribute("aria-disabled") != "true":
            driver.execute_script("arguments[0].scrollIntoView(true);", btn)
            driver.execute_script("arguments[0].click();", btn)
            return True
        else:
            print("      [pagination] last page reached")
            return False
    except Exception:
        print("      [pagination] next button not found")
        return False


def wait_for_cards(driver, timeout=20):
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, 'article[data-testid^="ProductCardWrapper-"]')
            )
        )
    except Exception:
        pass


# ── Scraping logic ─────────────────────────────────────────────────────────────

def parse_products(html, category_name, store_name):
    soup  = BeautifulSoup(html, "lxml")
    cards = soup.find_all("article", attrs={"data-testid": re.compile(r"^ProductCardWrapper-")})
    products = []

    for card in cards:
        card_testid = card.get("data-testid", "").replace("ProductCardWrapper-", "")

        name_el = card.find(attrs={"data-testid": re.compile(r"-ProductNameTestId$")})
        if not name_el:
            continue
        name = re.sub(
            r"Open product description$", "", name_el.get_text(strip=True), flags=re.IGNORECASE
        ).strip()

        link = card.find("a", class_=re.compile(r"ProductCardHiddenLink--"))
        product_url = (
            link["href"] if link and link.get("href")
            else f"{BASE_URL}/sm/product/en/{card_testid}"
        )

        product_id = uuid.uuid4().hex

        pricing_div = card.find(attrs={"data-testid": "productCardPricing-div-testId"})
        price = None
        if pricing_div:
            # On sale items Save-On-Foods uses a SalePrice/now-price class.
            # Explicitly prefer that element; fall back to the regular price class.
            price_el = (
                pricing_div.find(class_=re.compile(r"SalePrice--|sale-price|now-price|NowPrice--", re.I))
                or pricing_div.find(class_=re.compile(r"ProductCardPrice--"))
            )
            if price_el:
                try:
                    price = float(price_el.get_text(strip=True).replace("$", "").replace(",", ""))
                except ValueError:
                    pass
            # Fallback: if still no price, take the largest plausible dollar amount
            # in the pricing div. MIN floor filters out per-unit prices (e.g. $0.01/EA).
            # MAX cap filters out points/reward phantom values (e.g. $767, $1603).
            # max() gives the shelf price since unit prices are always smaller.
            if price is None:
                # Strip parenthesised per-unit price expressions like "($47.03 per 100g)"
                # before scanning for dollar amounts — these are NOT the shelf price.
                pricing_text = re.sub(
                    r"\(\s*\$[\d.]+\s+per\s+\d+\s*(?:g|ml|kg|l|oz|lb)\s*\)",
                    "", pricing_div.get_text(), flags=re.IGNORECASE
                )
                MIN_PLAUSIBLE_PRICE = 0.25
                MAX_PLAUSIBLE_PRICE = 150.0
                raw_prices = re.findall(r"\$\s*(\d+\.?\d*)", pricing_text)
                plausible = [float(p) for p in raw_prices if MIN_PLAUSIBLE_PRICE <= float(p) <= MAX_PLAUSIBLE_PRICE]
                if plausible:
                    try:
                        # Take the smallest remaining value — on sale cards the crossed-out
                        # was-price is larger than the now-price so min() gives the real price.
                        price = min(plausible)
                    except (ValueError, IndexError):
                        pass
                elif raw_prices:
                    try:
                        price = min(float(p) for p in raw_prices)
                    except ValueError:
                        pass

        # ── Stock status ───────────────────────────────────────────────────────
        card_html = safe_str(card)
        in_stock = not bool(re.search(
            r'out.of.stock|unavailable|not available|sold.out|'
            r'OutOfStock|SoldOut|'
            r'data-testid="[^"]*out-of-stock|data-testid="[^"]*unavailable',
            card_html, re.IGNORECASE
        ))
        if in_stock:
            # Save-On-Foods uses a specific testid for the add-to-cart button
            try:
                add_btn = card.find(attrs={"data-testid": re.compile(r"addToCart|add-to-cart", re.I)})
                if add_btn and (add_btn.get("disabled") or add_btn.get("aria-disabled") == "true"):
                    in_stock = False
            except (AttributeError, TypeError):
                pass

        # Search the image wrapper first; fall back to the whole card.
        # Sale cards inject badge elements that can shift the wrapper's DOM.
        img_wrapper = card.find(attrs={"data-testid": re.compile(r"^productCardImage_")})
        img_tag = img_wrapper.find("img") if img_wrapper else None
        if not img_tag:
            img_tag = card.find("img")
        image_url = img_tag.get("src", "") if img_tag else ""

        if not name or not price:
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


def select_store(driver, postal_code):
    """No-op: Save-On-Foods store selection is handled via rsid in the URL,
    not through a browser-based postal code flow. The runner calls this
    expecting a standard interface; we satisfy it here without any action."""
    pass


def scrape_category(driver, category_name, category_url, store_name):
    print(f"    Scraping: {category_name}")
    driver.get(category_url)
    wait_for_cards(driver)
    all_products, seen_ids, page = [], set(), 1

    while page <= MAX_PAGES:
        print(f"      Page {page}...", end=" ")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        page_products = parse_products(driver.page_source, category_name, store_name)
        new = [p for p in page_products if p["name"].lower() not in seen_ids]
        for p in new:
            seen_ids.add(p["name"].lower())
        all_products.extend(new)
        print(f"+{len(new)} ({len(all_products)} total)")
        if not new:
            break
        if not click_next(driver):
            break
        page += 1
        wait_for_cards(driver)
        time.sleep(2)

    return all_products


# ── Main ───────────────────────────────────────────────────────────────────────

def get_chrome_major_version():
    """Detect the installed Chrome major version number."""
    try:
        output = subprocess.check_output(
            ["google-chrome", "--version"], text=True
        ).strip()
        m = re.search(r"(\d+)\.", output)
        return int(m.group(1)) if m else None
    except Exception:
        return None


def main():
    #print(driver.capabilities['browserVersion'])
    # Create the table once before any scraping starts.
    # If the table already exists this does nothing — it never overwrites data.
    setup_database()

    # undetected_chromedriver bypasses Cloudflare bot detection.
    # Do NOT use headless — it gets detected and blocked.
    # Auto-detect Chrome version so it works across environments.
    chrome_version = get_chrome_major_version()
    options = uc.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    driver  = uc.Chrome(options=options, version_main=chrome_version)

    try:
        for loc_index, (store_name, store_id) in enumerate(LOCATIONS, 1):
            print(f"\n{'='*50}")
            print(f"Location [{loc_index}/{len(LOCATIONS)}]: {store_name} (rsid: {store_id})")
            print(f"{'='*50}")
            categories = [
                (name, f"{BASE_URL}/sm/planning/rsid/{store_id}/categories/{slug}")
                for name, slug in CATEGORIES
            ]
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
