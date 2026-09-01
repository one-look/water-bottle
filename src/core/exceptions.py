class CMSError(Exception):
    '''
    Base exception for water bottle
    '''
    pass

class ETLError(CMSError):
    '''
    Raised during ETL processing failures
    '''
    pass

class RAGError(CMSError):
    '''
    Raised during RAG query processing
    '''
    pass