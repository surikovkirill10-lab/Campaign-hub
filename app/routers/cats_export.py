# app/routers/cats_export.py
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from app.services.cats_export import (
    export_and_ingest,
    download_stat_file_by_id,
    parse_stat_bytes,
    _build_download_conf,
)

router = APIRouter()

@router.get("/cats/export/preview")
def cats_export_preview(id: str = Query(..., alias="id")):
    """
    Скачивает файл по ID, парсит в память и отдаёт только превью (первые 10 строк + колонки).
    RAW тоже сохраняется.
    """
    try:
        conf = _build_download_conf()
        p, content, meta = download_stat_file_by_id(id)
        df = parse_stat_bytes(content, fmt=conf["format"], encoding=conf["encoding"], delimiter=conf["delimiter"])
        head = df.head(10).to_dict(orient="records")
        return JSONResponse({"ok": True, "meta": meta, "columns": list(df.columns), "preview": head, "raw_path": str(p)})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/cats/export/ingest")
def cats_export_ingest(id: str = Query(..., alias="id")):
    """
    Полный цикл: скачать -> распарсить -> нормализовать -> сохранить normalized CSV.
    Возвращает путь и размер.
    """
    try:
        res = export_and_ingest(id)
        return JSONResponse(res)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
