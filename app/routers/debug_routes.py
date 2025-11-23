from fastapi import APIRouter, Request
router = APIRouter()

@router.get("/__routes")
def list_routes(request: Request):
    out = []
    for r in request.app.routes:
        try:
            path = getattr(r, "path", None) or getattr(r, "path_format", None)
            methods = sorted(list(getattr(r, "methods", set())))
            name = getattr(r, "name", "")
            out.append({"path": path, "methods": methods, "name": name})
        except Exception:
            pass
    return {"routes": out}
