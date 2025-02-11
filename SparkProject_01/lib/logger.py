class Log4J:
    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j #getting a JVM object from spark session for creating a log4j instance
        root_class = "sbdl.spark.example"
        conf = spark.sparkContext.getConf()
        app_name = conf.get("spark.app.name")
        self.logger = log4j.LogManager.getLogger(root_class + "."+app_name)  #creating a logger attribute initialize with log4j

    def warn(self, message):
        self.logger.warn(message)  # pass on the message to logger's warning method

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)

