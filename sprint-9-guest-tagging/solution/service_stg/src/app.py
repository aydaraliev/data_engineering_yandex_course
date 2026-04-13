import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from stg_loader.stg_message_processor_job import StgMessageProcessor
from stg_loader.repository.stg_repository import StgRepository

app = Flask(__name__)


# Register an endpoint for checking whether the service is up.
# It can be reached with a GET request at localhost:5000/health.
# If the response is 'healthy', the service is up and running.
@app.get('/health')
def health():
    return 'healthy'


if __name__ == '__main__':
    # Set the logging level to Debug so that debug logs can be viewed.
    app.logger.setLevel(logging.DEBUG)

    # Initialize the config. For convenience, the logic for reading environment variables
    # has been extracted into a dedicated class.
    config = AppConfig()

    # Initialize the message processor.
    # It is empty for now; message-handling logic for Kafka will be added later.
    proc = StgMessageProcessor(
        config.kafka_consumer(),
        config.kafka_producer(),
        config.redis_client(),
        StgRepository(config.pg_warehouse_db()),
        100,
        app.logger
    )

    # Run the processor in the background.
    # BackgroundScheduler will invoke the run method of our handler (StgMessageProcessor) on schedule.
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=config.DEFAULT_JOB_INTERVAL)
    scheduler.start()

    # Start the Flask application.
    app.run(debug=True, host='0.0.0.0', use_reloader=False)
