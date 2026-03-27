# ------------------------------------------------------------------
# Dockerfile -- Cartly scraper container
# Runs Selenium + headless Chrome + Python scrapers
# ------------------------------------------------------------------
FROM python:3.12-slim

# -- Install Chrome + ChromeDriver + lxml system deps ---------------
RUN apt-get update && apt-get install -y --no-install-recommends \
        wget gnupg2 unzip curl fonts-liberation libasound2 \
        libatk-bridge2.0-0 libatk1.0-0 libcups2 libdbus-1-3 \
        libdrm2 libgbm1 libgtk-3-0 libnspr4 libnss3 \
        libxcomposite1 libxdamage1 libxrandr2 xdg-utils \
        libxml2 libxslt1.1 \
    && wget -q -O /tmp/chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get install -y /tmp/chrome.deb \
    && rm /tmp/chrome.deb \
    # Install matching ChromeDriver via chrome-for-testing
    && CHROME_VERSION=$(google-chrome --version | grep -oP '\d+\.\d+\.\d+') \
    && DRIVER_URL=$(curl -s "https://googlechromelabs.github.io/chrome-for-testing/known-good-versions-with-downloads.json" \
         | python3 -c "import sys,json; vs=json.load(sys.stdin)['versions']; m=[v for v in vs if v['version'].startswith('${CHROME_VERSION}')]; print([d['url'] for d in m[-1]['downloads'].get('chromedriver',[]) if d['platform']=='linux64'][0])" 2>/dev/null \
         || echo "") \
    && if [ -n "$DRIVER_URL" ]; then \
         wget -q -O /tmp/chromedriver.zip "$DRIVER_URL" \
         && unzip /tmp/chromedriver.zip -d /tmp/cd \
         && mv /tmp/cd/*/chromedriver /usr/local/bin/ \
         && chmod +x /usr/local/bin/chromedriver \
         && rm -rf /tmp/chromedriver.zip /tmp/cd; \
       fi \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# -- Chrome environment for containers ------------------------------
ENV DBUS_SESSION_BUS_ADDRESS=/dev/null
ENV CHROME_NO_SANDBOX=1

# -- Python dependencies --------------------------------------------
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir setuptools && pip install --no-cache-dir -r requirements.txt

# -- Pre-patch undetected_chromedriver at build time -----------------
RUN python -c "import undetected_chromedriver as uc; p = uc.Patcher(); p.auto(); print('Chromedriver patched at', p.executable_path)"

# -- Application code -----------------------------------------------
COPY . .

# -- Entrypoint ------------------------------------------------------
CMD ["python", "run_all_scrapers.py"]
