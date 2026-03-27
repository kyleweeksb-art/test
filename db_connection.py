"""
db_connection.py — Shared MySQL connection helper (local testing version).

Uses plain username/password authentication instead of Azure Managed Identity.
Set the environment variables below, or edit the defaults directly.

Usage:
    from db_connection import get_db_connection

    connection = get_db_connection()
    cursor = connection.cursor()
    # ... use cursor ...
    connection.close()
"""

import os
import mysql.connector


# -- Configuration from environment variables ----------------------------------
MYSQL_HOST     = "cartlymysql.mysql.database.azure.com"
MYSQL_DATABASE = "products"
MYSQL_USER     = "cartlyadmin"
MYSQL_PASSWORD = "000129725Dd@23"
MYSQL_PORT     = 3306


def get_db_connection():
    """
    Return a mysql.connector connection using username/password auth.
    """
    connection = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        port=MYSQL_PORT,
        ssl_disabled=False,
    )
    return connection
