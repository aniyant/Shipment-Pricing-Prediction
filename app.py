from shipment.logger import logging
from shipment.exception import ShipmentException

from flask import Flask
import sys

app = Flask(__name__)

@app.route("/",methods=['GET','POST'])
def index():
    try:
        logging.info("Basic setup for shipment project is done.")
        return "Shipment project basic setup complete along with cicd."

    except Exception as e:
        raise ShipmentException(e,sys) from e


if __name__ == "__main__":
    app.run()