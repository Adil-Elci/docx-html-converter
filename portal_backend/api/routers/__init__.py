from .admin_users_routes import router as admin_users_router
from .auth_routes import router as auth_router
from .client_site_access_routes import router as client_site_access_router
from .clients_routes import router as clients_router
from .db_updater_routes import router as db_updater_router
from .jobs_routes import router as jobs_router
from .automation_routes import router as automation_router
from .site_credentials_routes import router as site_credentials_router
from .sites_routes import router as sites_router
from .submissions_routes import router as submissions_router

__all__ = [
    "auth_router",
    "admin_users_router",
    "automation_router",
    "clients_router",
    "db_updater_router",
    "sites_router",
    "site_credentials_router",
    "client_site_access_router",
    "submissions_router",
    "jobs_router",
]
