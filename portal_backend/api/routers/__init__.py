from .client_site_access_routes import router as client_site_access_router
from .clients_routes import router as clients_router
from .jobs_routes import router as jobs_router
from .site_credentials_routes import router as site_credentials_router
from .sites_routes import router as sites_router
from .submissions_routes import router as submissions_router

__all__ = [
    "clients_router",
    "sites_router",
    "site_credentials_router",
    "client_site_access_router",
    "submissions_router",
    "jobs_router",
]
