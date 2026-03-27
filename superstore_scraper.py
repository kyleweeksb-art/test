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

# ── Store locations (name, postal_code) ───────────────────────────────────────
LOCATIONS = [
    ("Country Village Road", "T3K5Z2"),
    ("4th Street",           "T2K1C3"),
    ("Signal Hill",          "T3H3P8"),
    ("Country Hills Boulevard",            "T3A5H8"),
    ("Westwinds Drive",          "T3J5K3"),
    ("Nursery",        "T3B2J8"),
    ("Heritage Meadows",          "T2H0B5"),
    ("20th Ave",        "T1Y6R3"),
    ("6th Ave",          "T2G0G7"),
    ("Macleod Trail",            "T2Y3R9"),
    ("Southport Road",          "T2W3N2"),
    ("Veterans Blvd",        "T4B3P2"),
    ("130th Ave",          "T2Z4E7"),
    ("Seton",          "T3M2N9"),
    ("Country Hills Town Centre",            "T3K5A5"),
    ("Coventry Hills Centre",          "T3K6A4"),
]

# ── Categories to scrape ───────────────────────────────────────────────────────
CATEGORIES = [
    # ── Fruits & Vegetables ───────────────────────────────────────────────────
    ("Fresh Fruit",                       "en/food/fruits-vegetables/fresh-fruit/c/28194"),
    ("Fresh Vegetables",                  "en/food/fruits-vegetables/fresh-vegetables/c/28195"),
    ("Packaged Salad & Dressing",         "en/food/fruits-vegetables/packaged-salad-dressing/c/28196"),
    ("Herbs",                             "en/food/fruits-vegetables/herbs/c/28197"),
    ("Fresh Cut Fruits & Vegetables",     "en/food/fruits-vegetables/fresh-cut-fruits-vegetables/c/28198"),
    ("Dried Fruits & Nuts",               "en/food/fruits-vegetables/dried-fruits-nuts/c/28199"),
    ("Fresh Juice & Smoothies",           "en/food/fruits-vegetables/fresh-juice-smoothies/c/28200"),

    # ── Dairy & Eggs ──────────────────────────────────────────────────────────
    ("Milk & Cream",                      "en/food/dairy-eggs/milk-cream/c/29224"),
    ("Eggs & Egg Substitutes",            "en/food/dairy-eggs/egg-egg-substitutes/c/29222"),
    ("Butter & Spreads",                  "en/food/dairy-eggs/butter-spreads/c/28220"),
    ("Cheese",                            "en/food/dairy-eggs/cheese/c/28225"),
    ("Yogurt",                            "en/food/dairy-eggs/yogurt/c/28227"),
    ("Desserts & Doughs",                 "en/food/dairy-eggs/desserts-doughs/c/28221"),
    ("Sour Cream & Dips",                 "en/food/dairy-eggs/sour-cream-dips/c/28226"),
    ("Lactose Free",                      "en/food/dairy-eggs/lactose-free/c/28223"),
    ("Non-Dairy Milk Alternatives",       "en/food/dairy-eggs/non-dairy-milk-alternatives/c/58904"),

    # ── Meat ─────────────────────────────────────────────────────────────────
    ("Chicken & Turkey",                  "en/food/meat/chicken-turkey/c/28214"),
    ("Beef",                              "en/food/meat/beef/c/28174"),
    ("Sausages",                          "en/food/meat/sausages/c/28170"),
    ("Bacon",                             "en/food/meat/bacon/c/59252"),
    ("Hot Dogs",                          "en/food/meat/hot-dogs/c/59253"),
    ("Pork & Ham",                        "en/food/meat/pork/c/28215"),
    ("Lamb & Veal",                       "en/food/meat/lamb-veal/c/28171"),
    ("Kebabs & Marinated Meat",           "en/food/meat/kebabs-marinated-meat/c/28173"),
    ("Game Meat, Offals & Fowl",          "en/food/meat/offals-game-meat-fowl/c/28216"),
    ("Plant Based Meat Alternatives",     "en/food/meat/plant-based-meat-alternatives/c/59318"),
    ("Deli Meat",                         "en/food/meat/deli-meat/c/59319"),

    # ── Pantry ────────────────────────────────────────────────────────────────
    ("Canned & Pickled",                  "en/food/pantry/canned-pickled-/c/28187"),
    ("Baking Essentials",                 "en/food/pantry/baking-essentials/c/28186"),
    ("Pasta & Pasta Sauce",               "en/food/pantry/pasta-pasta-sauce/c/28247"),
    ("Easy Meals & Sides",                "en/food/pantry/easy-meals-sides/c/28246"),  # fixed slug
    ("Cereal & Breakfast",                "en/food/pantry/cereal-breakfast/c/28183"),
    ("Honey, Syrups & Spreads",           "en/food/pantry/honey-syrups-spreads/c/28184"),
    ("Rice",                              "en/food/pantry/rice/c/28248"),
    ("Oils & Vinegar",                    "en/food/pantry/oils-vinegar/c/28244"),
    ("Condiments & Sauces",               "en/food/pantry/condiments/c/28243"),
    ("Spices & Seasonings",               "en/food/pantry/spices-seasonings/c/28188"),
    ("Dried Beans, Vegetables & Grains",  "en/food/pantry/dried-beans-vegetables-grains/c/28185"),
    ("Bulk Nuts and Candy",               "en/food/pantry/bulk/c/57088"),
    ("International Foods",               "en/food/pantry/international-foods/c/28245"),

    # ── International Foods ───────────────────────────────────────────────────
    ("East Asian Foods",                  "en/food/international-foods/east-asian-foods/c/58466"),
    ("South Asian Foods",                 "en/food/international-foods/south-asian-foods/c/58045"),
    ("Afro-Caribbean Foods",              "en/food/international-foods/caribbean-foods/c/58498"),
    ("Middle Eastern Foods",              "en/food/international-foods/middle-eastern-foods/c/58561"),
    ("Filipino Foods",                    "en/food/international-foods/filipino-foods/c/58812"),
    ("Latin American Foods",              "en/food/international-foods/latin-american-foods/c/58680"),
    ("European Foods",                    "en/food/international-foods/european-foods/c/58801"),

    # ── Snacks, Chips & Candy ─────────────────────────────────────────────────
    ("Chips & Snacks",                    "en/food/snacks-chips-candy/chips-snacks/c/28250"),
    ("Candy & Chocolate",                 "en/food/snacks-chips-candy/candy-chocolate/c/28249"),
    ("Crackers & Cookies",                "en/food/snacks-chips-candy/crackers-cookies/c/28242"),
    ("Snack Cakes",                       "en/food/snacks-chips-candy/snack-cakes/c/59210"),

    # ── Frozen ────────────────────────────────────────────────────────────────
    ("Ice Cream & Desserts",              "en/food/frozen/ice-cream-desserts/c/28240"),
    ("Frozen Fruit & Vegetables",         "en/food/frozen/fruit-vegetables/c/28162"),
    ("Frozen Meat & Seafood",             "en/food/frozen/frozen-meat-seafood/c/57003"),
    ("Frozen Meals, Entrees & Sides",     "en/food/frozen/meals-entrees-sides/c/28163"),
    ("Frozen Pizza",                      "en/food/frozen/frozen-pizza/c/28165"),
    ("Frozen Appetizers & Snacks",        "en/food/frozen/appetizers-snacks/c/28238"),
    ("Frozen Bakery & Breakfast",         "en/food/frozen/bakery-breakfast/c/28164"),
    ("Frozen Beverages & Ice",            "en/food/frozen/beverages-ice/c/28239"),
    ("Frozen Meatless Alternatives",      "en/food/frozen/meatless-alternatives/c/28241"),

    # ── Natural & Organic ─────────────────────────────────────────────────────
    ("Natural Cereals, Spreads & Syrups", "en/food/natural-and-organic/cereal-spreads-syrups/c/59260"),
    ("Natural Bakery",                    "en/food/natural-and-organic/bakery/c/59271"),
    ("Natural Condiments, Sauces & Oils", "en/food/natural-and-organic/condiments-sauces-and-oils/c/29173"),
    ("Natural Snacks, Chips & Candy",     "en/food/natural-and-organic/snacks-chips-candy/c/29174"),
    ("Natural Dairy & Eggs",              "en/food/natural-and-organic/dairy-and-eggs/c/59391"),
    ("Natural Drinks",                    "en/food/natural-and-organic/drinks/c/29717"),
    ("Natural Frozen Foods",              "en/food/natural-and-organic/c/59302"),
    ("Natural Baking & Spices",           "en/food/natural-and-organic/baking-and-spices/c/29924"),
    ("Natural Canned",                    "en/food/natural-and-organic/canned/c/29925"),
    ("Natural Pasta & Side Dishes",       "en/food/natural-and-organic/pasta-and-side-dishes/c/29927"),
    ("Natural Bars & Protein",            "en/food/natural-and-organic/bars-and-protein/c/59281"),

    # ── Bakery ────────────────────────────────────────────────────────────────
    ("Bread",                             "en/food/bakery/bread/c/28251"),
    ("Buns & Rolls",                      "en/food/bakery/buns-rolls/c/28147"),
    ("Cookies, Muffins & Desserts",       "en/food/bakery/cookies-muffins-desserts/c/28148"),
    ("Bagels, Croissants & English Muffins", "en/food/bakery/bagels-croissants-muffins/c/28149"),
    ("Wraps, Flatbread & Pizza Crust",    "en/food/bakery/wraps-flatbread-pizza-crust/c/28150"),
    ("Cakes",                             "en/food/bakery/cakes/c/59494"),

    # ── Prepared Meals ────────────────────────────────────────────────────────
    ("Rotisserie & Fried Chicken",        "en/food/prepared-meals/rotisserie-fried-chicken/c/28166"),
    ("Fresh Pasta & Sauce",               "en/food/prepared-meals/fresh-pasta-sauce/c/28210"),
    ("Entrees & Appetizers",              "en/food/prepared-meals/entrees-appetizers/c/28205"),
    ("Salads & Soups",                    "en/food/prepared-meals/salads-soups/c/28167"),
    ("Sandwiches",                        "en/food/prepared-meals/sandwiches/c/28208"),
    ("Sushi",                             "en/food/prepared-meals/sushi/c/28209"),
    ("Pizza",                             "en/food/prepared-meals/pizza/c/28211"),
    ("Quiches & Pies",                    "en/food/prepared-meals/quiches-pies/c/28206"),
    ("Snacks & Dips",                     "en/food/prepared-meals/snacks-dips/c/57043"),

    # ── Drinks ────────────────────────────────────────────────────────────────
    ("Juice",                             "en/food/drinks/juice/c/28230"),
    ("Coffee",                            "en/food/drinks/coffee/c/28228"),
    ("Tea & Hot Drinks",                  "en/food/drinks/tea-hot-drinks/c/28234"),
    ("Soft Drinks",                       "en/food/drinks/soft-drinks/c/28231"),
    ("Water",                             "en/food/drinks/water/c/28235"),
    ("Sports & Energy",                   "en/food/drinks/sports-energy/c/28223"),
    ("Drink Mixes",                       "en/food/drinks/drink-mixes/c/28229"),
    ("Non-Alcoholic Drinks",              "en/food/drinks/non-alcoholic-drinks/c/29718"),

    # ── Deli ──────────────────────────────────────────────────────────────────
    ("Deli Meat",                         "en/food/deli/deli-meat/c/28201"),
    ("Deli Cheese",                       "en/food/deli/deli-cheese/c/28202"),
    ("Dips, Spreads & Antipasto",         "en/food/deli/antipasto-dips-spreads/c/28203"),
    ("Crackers & Condiments",             "en/food/deli/crackers-condiments/c/28158"),
    ("Vegan & Vegetarian",                "en/food/deli/vegan-vegetarian/c/28204"),
    ("Lunch & Snack Kits",                "en/food/deli/lunch-snack-kits/c/57039"),
    ("Party Trays",                       "en/food/deli/party-trays/c/28212"),

    # ── Fish & Seafood ────────────────────────────────────────────────────────
    ("Shrimp",                            "en/food/fish-seafood/shrimp/c/28218"),
    ("Salmon",                            "en/food/fish-seafood/salmon/c/28217"),  # fixed: was c/2817
    ("Fish",                              "en/food/fish-seafood/fish/c/28191"),
    ("Smoked Fish",                       "en/food/fish-seafood/smoked-fish/c/28219"),
    ("Seafood Appetizers",                "en/food/fish-seafood/seafood-appetizers/c/28192"),
    ("Shellfish",                         "en/food/fish-seafood/shellfish/c/28190"),
    ("Squid & Octopus",                   "en/food/fish-seafood/squid-octopus/c/28193"),

]

BASE_URL   = "https://www.realcanadiansuperstore.ca"
STORE_NAME = "Real Canadian Superstore"
MAX_PAGES  = 50

# ── Name-cleaning helpers ──────────────────────────────────────────────────────

# Loblaw badge/label prefixes that appear before the brand+name
BADGE_PREFIXES = re.compile(
    r"^(Prepared in Canada|Raised without Antibiotics|Ocean Wise Recommended|"
    r"Certified Organic|Canadian|Organic|Gluten[- ]Free|Subscribe\s*&?\s*Earn\s*\S*|"
    r"PC Organics|PC Blue Menu|President's Choice|No Name|"
    r"Farmer'?s? Market|Kirkland|Great Value)\s*",
    re.IGNORECASE
)

# Known multi-word brands whose first word looks like a food descriptor
MULTIWORD_BRANDS = re.compile(
    r"^(Cracker\s+Barrel|Miss\s+Vickie'?s?|Old\s+Dutch|Old\s+El\s+Paso|"
    r"Tex\s+Mex|Gay\s+Lea|Aroy[- ]D|Old[- ]Fashioned|St\.\s*Albert|"
    r"CAVENDISH\s*FARMS|Country\s+Harvest|Compliments|Selection|Western\s+Family)(?=[\s\d]|$)",
    re.IGNORECASE
)

# Non-product strings to drop entirely (promo labels, store items)
JUNK_PATTERNS = re.compile(
    r"^(Subscribe\s*&|Reusable\s+Bag)",
    re.IGNORECASE
)

# Package size suffixes: "1LB", "3 lb Bag", ", 4 lb bag", " (5lb Bag)", "3Ct" etc.
SIZE_SUFFIX = re.compile(
    r"(?:"
    # paren-wrapped: " (5lb Bag)", " (1.36kg)"
    r"\s+\([^)]*\d+[^)]*(?:kg|g|lb|lbs|oz|ml|l|ea|each|pack|ct|pieces?|bag|bunch|count|pint|quart)[^)]*\)"
    # comma or space + numeric: "1LB", "3 lb Bag", ", 4 lb bag", "3Ct"
    r"|(?:,\s*|\s+)\d+(?:[xX]\d+)?(?:\.\d+)?\s*"
    r"(?:kg|g|lb|lbs|oz|ml|l|ea|each|pack|ct|piece|pieces|bag|bunch|count|pint|quart)"
    r"(?:\s+(?:bag|box|pack|bunch|case|tray))?"
    # word-only: "Bunch", "Half Pint", "Single", "Pint", "Bag", "Pack"
    r"|(?:,\s*|\s+)(?:half\s+pint|pint|quart|single|bunch|bag|pack|box|each)"
    r")\s*$",
    re.IGNORECASE
)


def clean_name(raw):
    """
    Loblaw concatenates badge text + brand + product name + package size into
    one string. Split on CamelCase boundaries, strip badges/sizes/junk, then
    isolate brand from the remaining product name.
    Returns (brand, clean_name).
    """
    if JUNK_PATTERNS.search(raw):
        return "", ""

    # Split CamelCase-joined spans: "CanadaPC" → "Canada PC"
    # Use [a-zà-ÿ] to also catch non-ASCII lowercase like ö: "IOGONanöDrinkable" → "IOGONanö Drinkable"
    spaced = re.sub(r"([a-zà-ÿ])([A-Z])", r"\1 \2", raw)
    # Split all-caps prefix from following TitleCase: "CAVENDISHFARMSClassic" → "CAVENDISHFARMS Classic"
    spaced = re.sub(r"([A-Z]{2,})([A-Z][a-z])", r"\1 \2", spaced)
    # Split single uppercase after hyphen from next word: "Aroy-DCanned" → "Aroy-D Canned"
    spaced = re.sub(r"(-[A-Z])([A-Z][a-z])", r"\1 \2", spaced)
    spaced = re.sub(r"\s+", " ", spaced).strip()

    # Strip trailing size suffixes (run 3× for stacked cases)
    for _ in range(3):
        spaced = SIZE_SUFFIX.sub("", spaced).strip()
    spaced = spaced.rstrip(",").strip()

    # Extract PC brand before BADGE_PREFIXES strips its sub-labels
    _pre = re.match(r"^(PC)\s+(?:Organics?|Blue\s+Menu)?\s*", spaced, re.IGNORECASE)
    pre_brand = _pre.group(1) if _pre else ""
    if pre_brand:
        spaced = spaced[_pre.end():].strip()

    # Strip badge/label prefixes (run 3× for stacked badges)
    for _ in range(3):
        spaced = BADGE_PREFIXES.sub("", spaced).strip()
    spaced = spaced.rstrip(",").strip()

    if not spaced:
        return pre_brand.strip(), ""

    # Check for known multi-word brands first
    mb = MULTIWORD_BRANDS.match(spaced)
    if mb:
        brand = mb.group(0).strip()
        # Normalize brands that may have been glued without spaces (e.g. "CAVENDISHFARMS")
        _BRAND_NORMALIZE = {"cavendishfarms": "CAVENDISH FARMS", "countryharvest": "Country Harvest"}
        brand = _BRAND_NORMALIZE.get(brand.lower().replace(" ", ""), brand)
        remainder = spaced[mb.end():].strip()
        return (pre_brand or brand).strip(), remainder

    # Words that should never be treated as a brand
    NOT_A_BRAND = re.compile(
        r"^(Bananas?|Strawberries|Blueberries|Raspberries|Blackberries|"
        r"Grapes?|Apples?|Oranges?|Lemons?|Limes?|Mangos?|Mangoes?|Avocados?|"
        r"Nectarines?|Plantains?|Papaya|Coconut|Figs?|Dates?|Grapefruit|Clementines?|"
        r"Pears?|Peaches?|Plums?|Cherries|Kiwis?|Pineapples?|Watermelon|Cantaloupe|"
        r"Bartlett|Gala|Fuji|Honeycrisp|McIntosh|Bosc|Ambrosia|Ataulfo|Navel|Mandarin|Granny|"
        r"Tomatoes?|Potatoes?|Onions?|Carrots?|Celery|Broccoli|Cauliflower|"
        r"Spinach|Lettuce|Peppers?|Cucumbers?|Zucchini|Mushrooms?|Garlic|Ginger|"
        r"Squash|Yams?|Asparagus|Corn|Peas|Beans|Eggplant|Beets?|Radish|Cabbage|"
        r"Kale|Romaine|Roma|Russet|Dragon|Jalapeno|Greenhouse|"
        r"Brussels\s+Sprouts|Artichoke|Leeks?|Fennel|Parsnip|Turnip|Bok\s+Choy|"
        r"Chicken|Turkey|Beef|Pork|Lamb|Salmon|Shrimp|Tuna|Tilapia|"
        r"Cheese|Butter|Milk|Eggs?|Yogurt|Cream|Bacon|Sausage|"
        r"Cheddar|Mozzarella|Gouda|Brie|Feta|Swiss|Havarti|Colby|Monterey|Marble|Parmesan|"
        r"Cottage|Pizza|Triple|"
        r"Ground|Bone-In|Boneless|Skinless|Lean|Breaded|Lightly|Fully|Colossal|Grass|"
        r"Raw|Sliced|Diced|Chopped|Minced|Shredded|Flaked|Roasted|Smoked|Split|"
        r"Pub|Popcorn|Salt|Sour|Barbecue|Ketchup|Dill|Buffalo|Kettle|Crisp|Baking|Tomato|Raspberry|Real|Club|Country|Grade|Hit|Old|Pulp|CLIF|"
        r"Fresh|Frozen|Organic|Natural|Large|Small|Extra|Whole|Mini|Baby|New|"
        r"Classic|Original|Premium|Select|Choice|Aged|Sharp|Mild|Light|Lite|Regular|"
        r"Spicy|Crispy|Crunchy|Creamy|Thin|Thick|Jumbo|Pure|Distilled|Pasteurized|"
        r"Unsalted|Salted|Sweetened|Unsweetened|Reduced|Lactose|Buttermilk|Evaporated|"
        r"Low[- ]Fat|Fat[- ]Free|All|Naturally|"
        r"Red|Green|Yellow|White|Black|Brown|Golden|Sweet|Purple|Pink|Crimson|Seedless|"
        r"Bag|Bunch|Pack|Box|Case|Dozen|Single|Half|Pint|Quart|Container|Tray|"
        r"Mixed|Assorted|\d.*)$",
        re.IGNORECASE
    )

    brand = pre_brand
    parts = spaced.split(" ", 1)
    if len(parts) == 2 and not brand:
        first = parts[0].rstrip(",")
        if (len(first) <= 20 and
                first[0].isupper() and
                not NOT_A_BRAND.match(first)):
            brand = first
            spaced = parts[1] if parts[1] else spaced

    # Strip trailing digits/symbols that got concatenated onto the brand
    # e.g. "Dairyland2%" → "Dairyland", "IOGO Nanö" stays intact
    brand = re.sub(r"[\d%]+$", "", brand).strip()

    return brand.strip(), spaced.strip()


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
    INSERT new rows, or UPDATE existing ones if the product_id already exists."""
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


def extract_price(text):
    """
    Pull a dollar price from text like '$2.25', 'about $0.92', 'about $3.82'.
    Ignores the 'about' prefix used on by-weight items.
    """
    text = re.sub(r"about\s*", "", text, flags=re.IGNORECASE).strip()
    m = re.search(r"\$\s*(\d+\.?\d*)", text)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass
    return None



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


# ── Browser helpers ────────────────────────────────────────────────────────────

def dismiss_popups(driver):
    try:
        driver.execute_script("""
            ['#onetrust-consent-sdk', '#onetrust-banner-sdk', '.onetrust-pc-dark-filter']
            .forEach(sel => { var el = document.querySelector(sel); if (el) el.remove(); });
        """)
    except Exception:
        pass


def select_store(driver, postal_code):
    """
    Store selection via the Loblaw store locator page.
    Navigates directly to the store locator, waits for the React app to
    fully render, types the postal code, and clicks SELECT LOCATION.
    """
    print(f"  Selecting store for postal code: {postal_code}")

    # ── Go straight to the store locator page ─────────────────────────────────
    driver.get(f"{BASE_URL}/en/store-locator?type=store")
    dismiss_popups(driver)
    time.sleep(5)
    dismiss_popups(driver)

    # ── Find and fill the postal code input ───────────────────────────────────
    INPUT_SELECTORS = [
        'input[placeholder*="address" i]',
        'input[placeholder*="postal" i]',
        'input[placeholder*="city" i]',
        'input[placeholder*="location" i]',
        'input[placeholder*="search" i]',
        'input[placeholder*="Enter" i]',
        'input[placeholder*="Find" i]',
        'input[aria-label*="address" i]',
        'input[aria-label*="postal" i]',
        'input[aria-label*="location" i]',
        'input[aria-label*="search" i]',
        'input[type="search"]',
        'input[name*="search" i]',
        'input[name*="location" i]',
        'input[id*="search" i]',
        'input[id*="location" i]',
        'input[id*="postal" i]',
        'input[id*="address" i]',
    ]

    inp_el = None

    for sel in INPUT_SELECTORS:
        try:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            for el in els:
                if el.is_displayed() and el.is_enabled():
                    inp_el = el
                    break
            if inp_el:
                break
        except Exception:
            pass

    if not inp_el:
        for sel in ['input[type="search"]', 'input[type="text"]']:
            try:
                inp_el = WebDriverWait(driver, 8).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, sel))
                )
                break
            except Exception:
                pass

    if not inp_el:
        try:
            all_inputs = driver.find_elements(By.TAG_NAME, "input")
            for el in all_inputs:
                t = el.get_attribute("type") or ""
                if t in ("text", "search", "") and el.is_displayed() and el.is_enabled():
                    inp_el = el
                    break
        except Exception:
            pass

    if not inp_el:
        print("  WARNING: Could not find postal code input. Dumping page info:")
        print(f"    URL: {driver.current_url}")
        try:
            all_inputs = driver.find_elements(By.TAG_NAME, "input")
            print(f"    Total <input> elements: {len(all_inputs)}")
            for i, el in enumerate(all_inputs[:8]):
                print(f"      [{i}] type={el.get_attribute('type')!r} "
                      f"placeholder={el.get_attribute('placeholder')!r} "
                      f"id={el.get_attribute('id')!r} "
                      f"name={el.get_attribute('name')!r} "
                      f"aria-label={el.get_attribute('aria-label')!r} "
                      f"visible={el.is_displayed()}")
        except Exception as e:
            print(f"    Debug dump failed: {e}")
        print("  Scraping with whatever store is currently set.")
        return False

    try:
        inp_el.click()
        time.sleep(0.5)
        inp_el.clear()
        inp_el.send_keys(postal_code)
        time.sleep(3)
    except Exception as e:
        print(f"  WARNING: Could not type into input: {e}")
        return False

    # ── Click SELECT LOCATION on the first result ─────────────────────────────
    BUTTON_SELECTORS = [
        'button[data-testid*="select-location" i]',
        'button[class*="select-location" i]',
        'button[class*="SelectLocation" i]',
        'button[class*="selectLocation" i]',
        '[data-testid*="store-result"] button',
        '[class*="store-result"] button',
        '[class*="StoreResult"] button',
        '[class*="store-card"] button',
        '[class*="StoreCard"] button',
        '[class*="store-list"] button',
        '[class*="location-list"] button',
    ]

    clicked_store = False
    for sel in BUTTON_SELECTORS:
        try:
            btn = WebDriverWait(driver, 8).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, sel))
            )
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(4)
            clicked_store = True
            break
        except Exception:
            pass

    if not clicked_store:
        try:
            btn = WebDriverWait(driver, 8).until(
                EC.element_to_be_clickable((By.XPATH,
                    '//button[contains(translate(.,"select location","SELECT LOCATION"),"SELECT LOCATION")]'
                    '| //button[contains(translate(.,"select","SELECT"),"SELECT")]'
                    '| //button[contains(translate(.,"choose","CHOOSE"),"CHOOSE")]'
                    '| //button[contains(translate(.,"set store","SET STORE"),"SET STORE")]'
                ))
            )
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(4)
            clicked_store = True
        except Exception:
            pass

    if not clicked_store:
        print("  WARNING: Could not click SELECT LOCATION. Dumping buttons:")
        try:
            btns = driver.find_elements(By.TAG_NAME, "button")
            for i, b in enumerate(btns[:10]):
                if b.is_displayed():
                    print(f"      [{i}] text={b.text[:80]!r} "
                          f"data-testid={b.get_attribute('data-testid')!r}")
        except Exception:
            pass
        print("  Scraping with whatever store is currently set.")
        return False

    print(f"  ✅ Store selected for {postal_code}.")
    return True


def wait_for_products(driver, timeout=20):
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href*="/p/"]'))
        )
    except Exception:
        pass


def scroll_page(driver):
    """Scroll slowly so the browser has time to lazy-load product images."""
    for i in range(1, 7):
        driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {i/6});")
        time.sleep(2)
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(3)
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(1)


def get_image_urls_js(driver):
    """
    After scrolling, collect all loaded product image URLs from the live DOM.
    Returns a dict of {product_code: image_url}.
    """
    return driver.execute_script(r"""
        var result = {};
        var base = window.location.origin;

        function bestSrc(img) {
            if (!img) return '';
            var src = img.src || '';
            if (src && !src.startsWith('data:') && src !== base + '/') return src;
            src = img.getAttribute('data-src') || img.getAttribute('data-lazy-src') || '';
            if (src) {
                if (src.startsWith('//')) src = 'https:' + src;
                if (src.startsWith('/'))  src = base + src;
                return src;
            }
            var ss = img.getAttribute('srcset') || img.getAttribute('data-srcset') || '';
            if (ss) {
                src = ss.split(',')[0].trim().split(' ')[0];
                if (src.startsWith('//')) src = 'https:' + src;
                if (src.startsWith('/'))  src = base + src;
                return src;
            }
            return '';
        }

        function isBadge(src) {
            return !src ||
                   src.includes('maple-leaf') ||
                   src.includes('badge') ||
                   src.includes('global-list') ||
                   src.includes('placeholder');
        }

        document.querySelectorAll('a[href*="/p/"]').forEach(function(link) {
            var href = link.getAttribute('href') || '';
            var m = href.match(/\/p\/(\d+)/);
            if (!m) return;
            var code = m[1];
            if (result[code]) return;
            var containers = [link, link.parentElement,
                              link.parentElement && link.parentElement.parentElement];
            for (var i = 0; i < containers.length; i++) {
                var c = containers[i];
                if (!c) continue;
                var imgs = c.querySelectorAll('img');
                for (var j = 0; j < imgs.length; j++) {
                    var src = bestSrc(imgs[j]);
                    if (src && !isBadge(src)) {
                        result[code] = src;
                        break;
                    }
                }
                if (result[code]) break;
            }
        });
        return result;
    """)


def click_next(driver):
    dismiss_popups(driver)
    for sel in [
        '[aria-label="Next page"]',
        '[aria-label="next page"]',
        '[data-testid*="next-page"]',
        '[data-testid*="nextPage"]',
    ]:
        try:
            btn = driver.find_element(By.CSS_SELECTOR, sel)
            disabled = (
                btn.get_attribute("disabled") or
                btn.get_attribute("aria-disabled") == "true" or
                "disabled" in (btn.get_attribute("class") or "")
            )
            if not disabled:
                driver.execute_script("arguments[0].scrollIntoView(true);", btn)
                driver.execute_script("arguments[0].click();", btn)
                return True
            else:
                print("      [pagination] last page reached")
                return False
        except Exception:
            pass
    return False


# ── Scraping logic ─────────────────────────────────────────────────────────────

def parse_products(html, image_map, category_name, store_name, postal_code):
    """
    Parse product cards from a Superstore category page.
    Uses a pre-collected image_map {product_code: url} from the live JS DOM.
    Names are cleaned via clean_name() to separate brand from product name.
    """
    soup  = BeautifulSoup(html, "lxml")
    links = soup.find_all("a", href=re.compile(r"/p/\d+", re.I))

    products = []
    seen     = set()

    for link in links:
        href         = link.get("href", "")
        product_url  = href if href.startswith("http") else BASE_URL + href

        m            = re.search(r"/p/(\d+)", href)
        product_code = m.group(1) if m else ""
        product_id   = uuid.uuid4().hex

        # Walk up to the card container (contains both price and name)
        card = link.parent
        for _ in range(4):
            if card is None:
                break
            text = card.get_text(" ", strip=True)
            if re.search(r"\$\d", text) and len(text) > 20:
                break
            card = card.parent

        if not card:
            continue

        card_text = card.get_text(" ", strip=True)

        # ── Raw name ──────────────────────────────────────────────────────────
        raw_name = link.get("aria-label", "").strip()
        if not raw_name:
            name_el = (
                card.find(class_=re.compile(r"product-name|ProductName|item-name|product-title", re.I)) or
                card.find(attrs={"data-testid": re.compile(r"product-name|ProductName", re.I)})
            )
            raw_name = name_el.get_text(strip=True) if name_el else ""
        if not raw_name:
            for el in link.find_all(["p", "span", "div"]):
                t = el.get_text(strip=True)
                if t and not re.search(r"^\$|^about\s*\$", t, re.I) and len(t) > 3:
                    raw_name = t
                    break

        # Strip concatenated unit-price text e.g. "Bananas$1.92/1kg" → "Bananas"
        raw_name = re.split(r"\$\d", raw_name)[0].strip().rstrip(",")
        # Save original (with size info intact) for parse_size before stripping
        raw_name_for_size = raw_name
        # Also strip bare size/unit strings appended without a $ separator
        # e.g. "Strawberries 1LB454 g", "Lemon1 ea", "Chicken Breasts1 ea"
        raw_name = re.sub(
            r"\d*\.?\d+\s*(?:kg|g|lb|lbs|oz|ml|l|ea|each)\s*$", "", raw_name, flags=re.IGNORECASE
        ).strip().rstrip(",")
        # Strip trailing pack-count suffix e.g. "Yogurt 1%6x" → "Yogurt 1%", "Broth4x" → "Broth"
        raw_name = re.sub(r"\s*\d+[xX]\s*$", "", raw_name).strip()
        brand, name = clean_name(raw_name)
        name = re.sub(r"\s+", " ", name).strip()

        if not name:
            continue

        # ── Price ──────────────────────────────────────────────────────────────
        price = None
        sale_el = card.find(class_=re.compile(r"sale|now-?price|promo|red|discount", re.I))
        if sale_el:
            price = extract_price(sale_el.get_text(strip=True))
        if price is None:
            price_el = card.find(class_=re.compile(
                r"selling-price|regular-price|price__value|product-price|ProductPrice", re.I))
            if price_el:
                price = extract_price(price_el.get_text(strip=True))
        if price is None:
            amounts = re.findall(r"(?:about\s*)?\$\s*(\d+\.\d{2})", card_text, re.IGNORECASE)
            if amounts:
                try:
                    price = float(amounts[0])
                except ValueError:
                    pass

        # ── Sanity-check price — filter out unit-price/points phantom values ───
        MAX_PLAUSIBLE_PRICE = 500.0
        if price is not None and price > MAX_PLAUSIBLE_PRICE:
            price = None

        # ── Stock status ───────────────────────────────────────────────────────
        card_html = safe_str(card)
        in_stock = not bool(re.search(
            r'out.of.stock|unavailable|not available|sold.out|'
            r'class="[^"]*out-of-stock|class="[^"]*unavailable',
            card_html, re.IGNORECASE
        ))
        if in_stock:
            try:
                add_btn = card.find(attrs={"aria-label": re.compile(r"add.*cart", re.I)})
                if add_btn and (add_btn.get("disabled") or add_btn.get("aria-disabled") == "true"):
                    in_stock = False
            except (AttributeError, TypeError):
                pass

        # ── Site unit price string ─────────────────────────────────────────────
        site_unit_price = ""
        unit_el = card.find(class_=re.compile(r"unit-?price|UnitPrice|per-unit|price-per", re.I))
        if unit_el:
            site_unit_price = unit_el.get_text(strip=True)

        # ── Image — use JS-collected map keyed by product code ─────────────────
        image_url = image_map.get(product_code, "")

        key = f"{name.lower()}|{product_url}"
        if key in seen:
            continue
        seen.add(key)

        if not name or not price:
            continue

        parsed_size, parsed_unit, package_size = parse_size(raw_name_for_size)
        cpu, qty, unit, unit_type = calc_unit_price(price, parsed_size, parsed_unit)
        unit_price_display = site_unit_price or (
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
            "brand":                       brand,
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


def scrape_category(driver, category_name, category_url, store_name, postal_code):
    print(f"    Scraping: {category_name}")
    driver.get(category_url)
    wait_for_products(driver)
    time.sleep(3)
    all_products, seen_ids, page = [], set(), 1

    while page <= MAX_PAGES:
        print(f"      Page {page}...", end=" ", flush=True)
        scroll_page(driver)

        # Collect image URLs from the live DOM via JS after scroll
        image_map = get_image_urls_js(driver)

        page_products = parse_products(
            driver.page_source, image_map, category_name, store_name, postal_code
        )
        new = [p for p in page_products if p["name"].lower() not in seen_ids]
        for p in new:
            seen_ids.add(p["name"].lower())
        all_products.extend(new)
        imgs_found = sum(1 for p in new if p["image_url"])
        print(f"+{len(new)} products, {imgs_found} images ({len(all_products)} total)")
        if not new:
            break
        if not click_next(driver):
            break
        page += 1
        wait_for_products(driver)
        time.sleep(3)

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
    setup_database()

    chrome_version = get_chrome_major_version()
    options = uc.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280,900")
    driver = uc.Chrome(options=options, version_main=chrome_version)

    try:
        for loc_index, (store_name, postal_code) in enumerate(LOCATIONS, 1):
            print(f"\n{'='*50}")
            print(f"Location [{loc_index}/{len(LOCATIONS)}]: {store_name} ({postal_code})")
            print(f"{'='*50}")
            select_store(driver, postal_code)
            print(f"  Using {len(CATEGORIES)} manually defined categories")
            loc_seen_ids = set()  # dedup sponsored cards across categories per location
            loc_total = 0
            for i, (cat_name, cat_slug) in enumerate(CATEGORIES, 1):
                print(f"\n  [{i}/{len(CATEGORIES)}]", end=" ")
                cat_url  = f"{BASE_URL}/{cat_slug}"
                products = scrape_category(driver, cat_name, cat_url, store_name, postal_code)
                if products:
                    new_products = [p for p in products if p["name"].lower() not in loc_seen_ids]
                    loc_seen_ids.update(p["name"].lower() for p in new_products)
                    skipped = len(products) - len(new_products)
                    if skipped:
                        print(f"    ({skipped} cross-category sponsored rows skipped)")
                    if new_products:
                        insert_products(new_products)
                        loc_total += len(new_products)
                else:
                    print("    No products found, skipping.")
            print(f"\n  Done {store_name}: {loc_total} products processed.")
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        driver = None

    print("\nAll locations complete.")


if __name__ == "__main__":
    main()