class CMSError(Exception):
    '''
    Base exception for water bottle
    '''
    pass

class RAGError(CMSError):
    '''
    Raised during RAG query processing
    '''
    pass