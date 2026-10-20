'''PostgreSQL engine lifecycle for the attendance database.'''

from typing import Optional

from sqlalchemy.engine import Engine

from src.core.logging import setup_logger

logger = setup_logger(__name__)

class AttendanceDatabase:
    '''
    Holds the shared SQLAlchemy engine for the attendance database.
    '''

    def __init__(self) -> None:
        logger.info("Initializing attendance database...")
        self._engine: Optional[Engine] = None

    def init(self, engine: Engine) -> None:
        '''
        Args:
            engine: SQLAlchemy engine from the SQL connector.
        '''
        logger.info("Attending database initialized.")
        self._engine = engine

    def close(self) -> None:
        logger.info("Closing attendance database...")
        if self._engine:
            self._engine.dispose()
            self._engine = None

    def is_ready(self) -> bool:
        logger.info("Checking if attendance database is ready...")
        return self._engine is not None

    @property
    def engine(self) -> Engine:
        logger.info("Getting attendance database engine...")
        if not self._engine:
            logger.error("Attendance database is not initialized.")
            raise RuntimeError("Attendance database is not initialized.")
        logger.info("Attendance database engine retrieved.")
        return self._engine


attendance_db = AttendanceDatabase()
