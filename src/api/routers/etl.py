import os
import tempfile
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, status, File, UploadFile
from src.core.multitenancy import get_current_tenant
from src.core.exceptions import CMSError
from src.etl.extractors import LocalDocumentExtractor
from src.etl.vectorstore import QdrantStoreProvider
from src.etl.pipeline import ETLPipeline

router = APIRouter(tags=["RAG & ETL"])
store_provider = QdrantStoreProvider()

@router.post("/etl/ingest", status_code=status.HTTP_202_ACCEPTED)
async def ingest_documents(
    file: UploadFile = File(...), 
    tenant_id: str = Depends(get_current_tenant)
):
    try:
        pipeline = ETLPipeline(
            extractor=LocalDocumentExtractor(),
            store_provider=store_provider
        )
        
        filename = file.filename or "upload"
        suffix = Path(filename).suffix

        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            contents = await file.read()
            tmp.write(contents)
            tmp_path = tmp.name

        try:
            pipeline.run(source_path=tmp_path, tenant_id=tenant_id)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        
        return {
            "status": "Ingestion successful", 
            "filename": filename, 
            "tenant_id": tenant_id
        }
    except CMSError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=str(e)
        )