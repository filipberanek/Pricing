import logging as Log

class logging:
    def __init__(self):
        self.logger = Log.getLogger()
        self.logger.setLevel(Log.INFO)
        
    def insert_into_log(self,type, message):
        if type.lower() == 'error':
            self.logger.error(message)
        elif type.lower() == 'warning':
            self.logger.warning(message)
        else:
            self.logger.info(message)

