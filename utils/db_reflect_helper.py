from sqlalchemy import MetaData

class ReflectHelper :
    
    @staticmethod
    def db_reflect(engine):
        metadata = MetaData()
        metadata.reflect(bind=engine)
        return metadata
        