"""
Job Context
"""
import utils.spark_utils as su


class JobContext(object):
    """
    Provides a context to access frequently used objects/properties within 
    an ETL workflow.

    Methods:
    __init__ - class constructor
    __enter__ - method to establish context
    __exit__ - method to gracefully exit context

    """

    def __init__(self, app_name, environment, snapshot_dt):
        """
        Class Constructor

        :param app_name: The name of the Spark Application
        :param environment: The environment the application will run in
        :snapshot_dt: The datetime when the application was run, which will
           dictate the lowest partition 
        """
        self.app_name = app_name
        self.environment = environment
        self.spark = su.get_spark(app_name)
        self.logger = su.get_logger(self.spark)
        self.snapshot_dt = snapshot_dt

        # Environment-specific variables will be set here
        self.bucket = "fsd-test-1" # Testing bucket

    def __enter__(self):
        """Method to establish context"""
        # print("Executing snapshot date {}".format(self.snapshot_dt))
        self.logger.info(
                "Executing {} {} for snapshot date {}...".format(
                    self.environment, self.app_name, self.snapshot_dt))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Method to gracefully exit context

        :param exc_type: Exception Type
        :param exc_val: Exception Value
        :param exc_tb: Exception Tracback
        """
        if exc_type:
            self.spark.stop()
            # print("Terminating run for snapshot date {}".format(self.snapshot_dt))
            self.logger.info("Terminating run {} {} for snapshot date {}"
                    " with ERROR {}: {}".format(self.environment, self.app_name,
                        self.snapshot_dt, exc_type.__name__, exc_val))
        else:
            self.spark.stop()
            # print("Finished run for snapshot date {}".format(self.snapshot_dt))
            self.logger.info("Finished run {} {} for snapshot date {}".format(
                self.environment, self.app_name, self.snapshot_dt))
