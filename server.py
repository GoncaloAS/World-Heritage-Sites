import logging  # configuring logging
from app import APP  # this is the Flask, o que abre o site, app instance imported from app.py
import db  # importing the database connection

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    db.connect()  # connecting to the database
    APP.run(host='0.0.0.0', port=9001, debug=True)  # running the app on port 9001
